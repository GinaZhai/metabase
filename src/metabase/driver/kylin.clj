(ns metabase.driver.kylin
    "Kylin driver. Builds off of the Generic SQL driver."
    (:require [clj-time
               [coerce :as tcoerce]
               [core :as t]
               [format :as time]]
      [clojure
       [set :as set]
       [string :as str]]
      [cheshire.core :as json]
      [clj-http.client :as http]
      [clojure.tools.logging :as log]
      [honeysql.core :as hsql]
      [metabase.driver.kylin.query-processor :as qp]
      [metabase
       [driver :as driver]
       [util :as u]]
      [metabase.db.spec :as dbspec]
      [metabase.driver.generic-sql :as sql]
      [metabase.driver.generic-sql.query-processor :as sqlqp]
      [metabase.util
       [date :as du]
       [honeysql-extensions :as hx]
       [i18n :refer [tru]]
       [ssh :as ssh]]
      [schema.core :as s])
    (:import java.sql.Time
      [java.util Date TimeZone]
      metabase.util.honeysql_extensions.Literal
      org.joda.time.format.DateTimeFormatter))

(defrecord KylinDriver []
           :load-ns true
           clojure.lang.Named
           (getName [_] "Kylin"))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  METHOD IMPLS                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- column->base-type [column-type]
       ({:BIGINT     :type/BigInteger
         :BINARY     :type/*
         :BIT        :type/Boolean
         :BLOB       :type/*
         :CHAR       :type/Text
         :DATE       :type/Date
         :DATETIME   :type/DateTime
         :DECIMAL    :type/Decimal
         :DOUBLE     :type/Float
         :ENUM       :type/*
         :FLOAT      :type/Float
         :INT        :type/Integer
         :INTEGER    :type/Integer
         :LONGBLOB   :type/*
         :LONGTEXT   :type/Text
         :MEDIUMBLOB :type/*
         :MEDIUMINT  :type/Integer
         :MEDIUMTEXT :type/Text
         :NUMERIC    :type/Decimal
         :REAL       :type/Float
         :SET        :type/*
         :SMALLINT   :type/Integer
         :TEXT       :type/Text
         :TIME       :type/Time
         :TIMESTAMP  :type/DateTime
         :TINYBLOB   :type/*
         :TINYINT    :type/Integer
         :TINYTEXT   :type/Text
         :VARBINARY  :type/*
         :VARCHAR    :type/Text
         :YEAR       :type/Integer} (keyword (str/replace (name column-type) #"\sUNSIGNED$" "")))) ; strip off " UNSIGNED" from end if present

(def ^:private ^:const default-connection-args
  "Map of args for the Kylin JDBC connection string.
   Full list of is options is available here: http://kylin.apache.org/docs/tutorial/setup_jdbc_datasource.html"
  {;; 0000-00-00 dates are valid in MySQL; convert these to `null` when they come back because they're illegal in Java
   :zeroDateTimeBehavior  :convertToNull
   ;; Force UTF-8 encoding of results
   :useUnicode            :true
   :characterEncoding     :UTF8
   :characterSetResults   :UTF8
   ;; Needs to be true to set useJDBCCompliantTimezoneShift to true
   :useLegacyDatetimeCode :true
   ;; This allows us to adjust the timezone of timestamps as we pull them from the resultset
   ;;:useJDBCCompliantTimezoneShift :true
   })

(def ^:private ^:const ^String default-connection-args-string
  (str/join \& (for [[k v] default-connection-args]
                    (str (name k) \= (name v)))))

(defn- append-connection-args
       "Append `default-connection-args-string` to the connection string in CONNECTION-DETAILS, and an additional option to
       explicitly disable SSL if appropriate. (Newer versions of Kylin will complain if you don't explicitly disable SSL.)"
       {:argslist '([connection-spec details])}
       [connection-spec {ssl? :ssl}]
       (update connection-spec :subname
               (fn [subname]
                   (let [join-char (if (str/includes? subname "?") "&" "?")]
                        (str subname join-char default-connection-args-string (when-not ssl?
                                                                                        "&useSSL=false"))))))
(defn- details->url
       "Helper for building a Kylin URL.

         (details->url {:host \"http://localhost\", :port 7070} \"kylin/v2\") -> \"http://localhost:7070/kylin/v2\""
       [{:keys [host port]} & strs]
       {:pre [(string? host) (seq host) (integer? port)]}
       (apply str (format "%s:%d" host port) (map name strs)))

(defn- can-connect? [details]
       {:pre [(map? details)]}
       (ssh/with-ssh-tunnel [details-with-tunnel details]
                            (= 200 (:status (http/get (details->url details-with-tunnel "/kylin")))))) ;;/api/tables_and_columns?project=metabasepro

(defn- connection-details->spec [details]
       (-> details
           ;;(set/rename-keys {:dbname :db})
           dbspec/kylin
           (append-connection-args details)
           (sql/handle-additional-options details)))

(defn- unix-timestamp->timestamp [expr seconds-or-milliseconds]
       (hsql/call :from_unixtime (case seconds-or-milliseconds
                                       :seconds      expr
                                       :milliseconds (hx// expr 1000))))

(defn- date-format [format-str expr] (hsql/call :date_format expr (hx/literal format-str)))
(defn- str-to-date [format-str expr] (hsql/call :str_to_date expr (hx/literal format-str)))

(def ^:private ^DateTimeFormatter timezone-offset-formatter
  "JodaTime formatter that returns just the raw timezone offset, e.g. `-08:00` or `+00:00`."
  (time/formatter "ZZ"))

(defn- timezone-id->offset-str
       "Get an appropriate timezone offset string for a timezone with `timezone-id` and `date-time`. MySQL only accepts
       these offsets as strings like `-8:00`.

           (timezone-id->offset-str \"US/Pacific\", date-time) ; -> \"-08:00\"

       Returns `nil` if `timezone-id` is itself `nil`. The `date-time` must be included as some timezones vary their
       offsets at different times of the year (i.e. daylight savings time)."
       [^String timezone-id date-time]
       (when timezone-id
             (time/unparse (.withZone timezone-offset-formatter (t/time-zone-for-id timezone-id)) date-time)))

(def ^:private ^TimeZone utc   (TimeZone/getTimeZone "UTC"))
(def ^:private utc-hsql-offset (hx/literal "+00:00"))

;; TODO - Should this go somewhere more general, like util ?
(defn- do-request
       "Perform a JSON request using REQUEST-FN against URL.
        Tho

          (do-request http/get \"http://my-json-api.net\")"
       [request-fn url & {:as options}]
       {:pre [(fn? request-fn) (string? url)]}
       (let [options               (cond-> (merge {:content-type "application/json" :basic-auth ["ADMIN" "KYLIN"]} options)
                                           (:body options) (update :body json/generate-string))
             {:keys [status body]} (request-fn url options)]
            (when (not= status 200)
                  (throw (Exception. (format "Error [%d]: %s" status body))))
            (try (json/parse-string body keyword)
                 (catch Throwable _
                   (throw (Exception. (str "Failed to parse body: " body)))))))

(def ^:private ^{:arglists '([url & {:as options}])} GET  (partial do-request http/get))
(def ^:private ^{:arglists '([url & {:as options}])} POST (partial do-request http/post))
(def ^:private ^{:arglists '([url & {:as options}])} DELETE (partial do-request http/delete))

(s/defn ^:private create-hsql-for-date
        "Returns an HoneySQL structure representing the date for MySQL. If there's a report timezone, we need to ensure the
        timezone conversion is wrapped around the `date-literal-or-string`. It supports both an `hx/literal` and a plain
        string depending on whether or not the date value should be emedded in the statement or separated as a prepared
        statement parameter. Use a string for prepared statement values, a literal if you want it embedded in the statement"
        [date-obj :- java.util.Date
         date-literal-or-string :- (s/either s/Str Literal)]
        (let [date-as-dt                 (tcoerce/from-date date-obj)
              report-timezone-offset-str (timezone-id->offset-str (driver/report-timezone) date-as-dt)]
             (if (and report-timezone-offset-str
                      (not (.hasSameRules utc (TimeZone/getTimeZone (driver/report-timezone)))))
               ;; if we have a report timezone we want to generate SQL like convert_tz('2004-01-01T12:00:00','-8:00','-2:00')
               ;; to convert our timestamp from the UTC timezone -> report timezone. Note `date-object-literal` is assumed to be
               ;; in UTC as `du/format-date` is being used which defaults to UTC.
               ;; See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_convert-tz
               ;; (We're using raw offsets for the JVM/report timezone instead of the timezone ID because we can't be 100% sure that
               ;; MySQL will accept either of our timezone IDs as valid.)
               ;;
               ;; Note there's a small chance that report timezone will never be set on the MySQL connection, if attempting to
               ;; do so fails because the ID is valid; if the report timezone is different from the MySQL database's timezone,
               ;; this will result in the `convert_tz()` call below being incorrect. Unfortunately we don't currently have a
               ;; way to determine that setting a timezone has failed for the current query, since it actualy is attempted
               ;; after the query is compiled. Hopefully situtations where that happens are rare; at any rate it's probably
               ;; preferable to have timezones slightly wrong in these rare theoretical situations, instead of all the time, as
               ;; was the previous behavior.
               (hsql/call :convert_tz
                          date-literal-or-string
                          utc-hsql-offset
                          (hx/literal report-timezone-offset-str))
               ;; otherwise if we don't have a report timezone we can continue to pass the object as-is, e.g. as a prepared
               ;; statement param
               date-obj)))

;; MySQL doesn't seem to correctly want to handle timestamps no matter how nicely we ask. SAD! Thus we will just
;; convert them to appropriate timestamp literals and include functions to convert timezones as needed
(defmethod sqlqp/->honeysql [KylinDriver Date]
           [_ date]
           (create-hsql-for-date date (hx/literal (du/format-date :date-hour-minute-second-ms date))))

;; The sqlqp/->honeysql entrypoint is used by MBQL, but native queries with field filters have the same issue. Below
;; will return a map that will be used in the prepared statement to correctly convert and parameterize the date
(s/defmethod sql/->prepared-substitution [KylinDriver Date] :- sql/PreparedStatementSubstitution
             [_ date]
             (let [date-str (du/format-date :date-hour-minute-second-ms date)]
                  (sql/make-stmt-subs (-> (create-hsql-for-date date date-str)
                                          hx/->date
                                          (hsql/format :quoting :kylin, :allow-dashed-names? true)
                                          first)
                                      [date-str])))

(defmethod sqlqp/->honeysql [KylinDriver Time]
           [_ time-value]
           (hx/->time time-value))

;; Since MySQL doesn't have date_trunc() we fake it by formatting a date to an appropriate string and then converting
;; back to a date. See http://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format for an
;; explanation of format specifiers
(defn- trunc-with-format [format-str expr]
       (str-to-date format-str (date-format format-str expr)))

(defn- date [unit expr]
       (case unit
             :default         expr
             :minute          (trunc-with-format "%Y-%m-%d %H:%i" expr)
             :minute-of-hour  (hx/minute expr)
             :hour            (trunc-with-format "%Y-%m-%d %H" expr)
             :hour-of-day     (hx/hour expr)
             :day             (hsql/call :date expr)
             :day-of-week     (hsql/call :dayofweek expr)
             :day-of-month    (hsql/call :dayofmonth expr)
             :day-of-year     (hsql/call :dayofyear expr)
             ;; To convert a YEARWEEK (e.g. 201530) back to a date you need tell MySQL which day of the week to use,
             ;; because otherwise as far as MySQL is concerned you could be talking about any of the days in that week
             :week            (str-to-date "%X%V %W"
                                           (hx/concat (hsql/call :yearweek expr)
                                                      (hx/literal " Sunday")))
             ;; mode 6: Sunday is first day of week, first week of year is the first one with 4+ days
             :week-of-year    (hx/inc (hx/week expr 6))
             :month           (str-to-date "%Y-%m-%d"
                                           (hx/concat (date-format "%Y-%m" expr)
                                                      (hx/literal "-01")))
             :month-of-year   (hx/month expr)
             ;; Truncating to a quarter is trickier since there aren't any format strings.
             ;; See the explanation in the H2 driver, which does the same thing but with slightly different syntax.
             :quarter         (str-to-date "%Y-%m-%d"
                                           (hx/concat (hx/year expr)
                                                      (hx/literal "-")
                                                      (hx/- (hx/* (hx/quarter expr)
                                                                  3)
                                                            2)
                                                      (hx/literal "-01")))
             :quarter-of-year (hx/quarter expr)
             :year            (hx/year expr)))

(defn- date-interval [unit amount]
       (hsql/call :date_add
                  :%now
                  (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))

;;; ### Query Processing

(defn- do-query [details query]
       {:pre [(map? details) (map? query)]}
       (ssh/with-ssh-tunnel [details-with-tunnel details]
                            (try
                              (POST (details->url details-with-tunnel "/kylin/api/query"), :body query)
                              (catch Throwable e
                                ;; try to extract the error
                                (let [message (or (u/ignore-exceptions
                                                    (when-let [body (json/parse-string (:body (:object (ex-data e))) keyword)]
                                                              (str (:error body) "\n"
                                                                   (:errorMessage body) "\n"
                                                                   "Error class:" (:errorClass body))))
                                                  (.getMessage e))]
                                     (log/error (u/format-color 'red "Error running query:\n%s" message))
                                     ;; Re-throw a new exception with `message` set to the extracted message
                                     (throw (Exception. message e)))))))

(defn- do-query-with-cancellation [details query]
       {:pre [(map? details) (map? query)]}
       (let [query-id  (get-in query [:context :queryId])
             query-fut (future (do-query details query))]
            (try
              ;; Run the query in a future so that this thread will be interrupted, not the thread running the query (which is
              ;; not interrupt aware)
              @query-fut
              (catch InterruptedException interrupted-ex
                ;; The future has been cancelled, if we ahve a query id, try to cancel the query
                (if-not query-id
                        (log/warn interrupted-ex "Client closed connection, no queryId found, can't cancel query")
                        (ssh/with-ssh-tunnel [details-with-tunnel details]
                                             (log/warnf "Client closed connection, cancelling Kylin queryId '%s'" query-id)
                                             (try
                                               ;; If we can't cancel the query, we don't want to hide the original exception, attempt to cancel, but if
                                               ;; we can't, we should rethrow the InterruptedException, not an exception from the cancellation
                                               (DELETE (details->url details-with-tunnel (format "/kylin/api/query/%s" query-id)))
                                               (catch Exception cancel-ex
                                                 (log/warnf cancel-ex "Failed to cancel kylin query with queryId" query-id))
                                               (finally
                                                 ;; Propogate the exception, will cause any other catch/finally clauses to fire
                                                 (throw interrupted-ex)))))))))

(defn- do-segment-metadata-query [details datasource]
       {:pre [(map? details)]}
       (do-query details {"queryType"     "segmentMetadata"
                          "dataSource"    datasource
                          "intervals"     ["1999-01-01/2114-01-01"]
                          "analysisTypes" []
                          "merge"         true}))

(defn- kylin-type->base-type [field-type]
       (case field-type
             "STRING"      :type/Text
             "FLOAT"       :type/Float
             "LONG"        :type/Integer
             "hyperUnique" :type/KylinHyperUnique
             :type/Float))

(defn- describe-table-field [field-name {field-type :type, :as info}]
       ;; all dimensions are Strings, and all metrics as JS Numbers, I think (?)
       ;; string-encoded booleans + dates are treated as strings (!)
       {:name          (name field-name)
        :base-type     (kylin-type->base-type field-type)
        :database-type field-type})

(defn- describe-table [database table]
       ;;{:pre [(map? (:details database))]}
       ;; (ssh/with-ssh-tunnel [details-with-tunnel (:details database)]
       ;; (let [kylin-datasources (GET (details->url details-with-tunnel "/kylin/api/tables_and_columns?project=metabasepro"))]
       ;; (get (json/parse-string kylin-datasources true) :table_NAME)
       ;; {:tables (set (for [table-name kylin-datasources]
       ;;                {:schema nil, :name table-name}))})))
       (ssh/with-ssh-tunnel [details-with-tunnel (:details database)]
                            (let [{:keys [columns]} (first (do-segment-metadata-query details-with-tunnel (:name table)))]
                                 {:schema nil
                                  :name   (:name table)
                                  :fields (set (concat
                                                 ;; every Kylin table is an event stream w/ a timestamp field
                                                 [{:name          "timestamp"
                                                   :database-type "timestamp"
                                                   :base-type     :type/DateTime
                                                   :pk?           true}]
                                                 (for [[field-name field-info] (dissoc columns :__time)]
                                                      (describe-table-field field-name field-info))))})))

(defn- describe-database [database]
       {:pre [(map? (:details database))]}
       (ssh/with-ssh-tunnel [details-with-tunnel (:details database)]
                            (let [kylin-datasources (GET (details->url details-with-tunnel "/kylin/api/tables_and_columns?project=metabasepro"))]
                                 (def databases (get (json/parse-string kylin-datasources true) :table_SCHEM))
                                 {:schema (set (for [database-name databases]
                                                    {:name database-name}))})))

(defn- humanize-connection-error-message [message]
       (condp re-matches message
              #"^Communications link failure\s+The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.$"
              (driver/connection-error-messages :cannot-connect-check-host-and-port)

              #"^Unknown database .*$"
              (driver/connection-error-messages :database-name-incorrect)

              #"Access denied for user.*$"
              (driver/connection-error-messages :username-or-password-incorrect)

              #"Must specify port after ':' in connection string"
              (driver/connection-error-messages :invalid-hostname)

              #".*" ; default
              message))

(defn- string-length-fn [field-key]
       (hsql/call :char_length field-key))

(def ^:private kylin-date-formatters
  (driver/create-db-time-formatters "yyyy-MM-dd HH:mm:ss.SSSSSS"))

(def ^:private kylin-db-time-query
  "select CURRENT_TIMESTAMP()")


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                        IDRIVER & ISQLDRIVER METHOD MAPS                                        |
;;; +----------------------------------------------------------------------------------------------------------------+

(u/strict-extend KylinDriver
                 driver/IDriver
                 (merge driver/IDriverDefaultsMixin
                        {:can-connect?      (u/drop-first-arg can-connect?)
                         ;;:date-interval  (u/drop-first-arg date-interval)
                         :describe-database (u/drop-first-arg describe-database)
                         :describe-table    (u/drop-first-arg describe-table)
                         :details-fields (constantly (ssh/with-tunnel-config
                                                       [driver/default-host-details
                                                        (assoc driver/default-port-details :default 7070)
                                                        (assoc driver/default-dbname-details :default "default")
                                                        {:name         "project"
                                                         :display-name (tru "Project")
                                                         :placeholder  (tru "default")
                                                         :required     true
                                                         :default "default"}
                                                        {:name "Basic QURNSU46S1lMSU4="
                                                         :display-name "authorization"
                                                         :placeholder  "authorization"
                                                         :required     true
                                                         :default "authorization"}
                                                        ;;driver/default-user-details
                                                        ;;driver/default-password-details
                                                        ;; (assoc driver/default-additional-options-details
                                                        ;;:placeholder "tinyInt1isBit=false")
                                                        ]))
                         :execute-query     (fn [_ query] (qp/execute-query do-query-with-cancellation query))
                         :humanize-connection-error-message (u/drop-first-arg humanize-connection-error-message)
                         ;;:current-db-time (driver/make-current-db-time-fn kylin-db-time-query kylin-date-formatters)
                         :features                          (fn [this]
                                                                ;; MySQL LIKE clauses are case-sensitive or not based on whether the
                                                                ;; collation of the server and the columns themselves. Since this isn't
                                                                ;; something we can really change in the query itself don't present the
                                                                ;; option to the users in the UI
                                                                (conj (sql/features this) :no-case-sensitivity-string-filter-options))
                         :mbql->native      (u/drop-first-arg qp/mbql->native)})

                 sql/ISQLDriver
                 (merge
                   (sql/ISQLDriverDefaultsMixin)
                   {:active-tables             sql/post-filtered-active-tables
                    :column->base-type         (u/drop-first-arg column->base-type)
                    :connection-details->spec  (u/drop-first-arg connection-details->spec)
                    :date                      (u/drop-first-arg date)
                    :excluded-schemas          (constantly #{"INFORMATION_SCHEMA"})
                    :quote-style               (constantly :kylin)
                    :string-length-fn          (u/drop-first-arg string-length-fn)
                    ;; If this fails you need to load the timezone definitions from your system into MySQL; run the command
                    ;; `mysql_tzinfo_to_sql /usr/share/zoneinfo | mysql -u root mysql` See
                    ;; https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html for details
                    ;; TODO - This can also be set via `sessionVariables` in the connection string, if that's more useful (?)
                    :set-timezone-sql          (constantly "SET @@session.time_zone = %s;")
                    :unix-timestamp->timestamp (u/drop-first-arg unix-timestamp->timestamp)}))

(defn -init-driver
      "Register the Kylin driver"
      []
      (driver/register-driver! :kylin (KylinDriver.)))
