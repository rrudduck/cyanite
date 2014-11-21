(ns io.cyanite.carbon
  "Dead simple carbon protocol handler"
  (:require [clojure.string        :as s]
            [io.cyanite.store      :as store]
            [io.cyanite.path       :as path]
            [io.cyanite.tcp        :as tcp]
            [io.cyanite.util       :refer [partition-or-time]]
            [clojure.tools.logging :refer [info debug]]
            [clojure.core.async    :as async :refer [<! >! >!! go chan]]))

(set! *warn-on-reflection* true)

(defn parse-num
  "parse a number into the given value, return the
  default value if it fails"
  [parse default number]
  (try (parse number)
    (catch Exception e
      (debug "got an invalid number" number (.getMessage e))
      default)))

(defn formatter
  "Split each line on whitespace, discard nan metric lines
   and format correct lines for each resolution"
  [rollups ^String input]
  (try
    (let [[path metric time] (s/split (.trim input) #" ")
          timel (parse-num #(Long/parseLong %) "nan" time)
          metricd (parse-num #(Double/parseDouble %) "nan" metric)]
      (when (and (not= "nan" metricd) (not= "nan" timel))
          (for [{:keys [rollup period ttl rollup-to]} rollups]
            {:path   (s/lower-case path)
             :rollup rollup
             :period period
             :ttl    (or ttl (* rollup period))
             :time   (rollup-to timel)
             :metric metricd})))
      (catch Exception e
          (info "Exception for metric [" input "] : " e))))

(defn format-processor
  "Send each metric over to the cassandra store"
  [chan indexch rollups insertch]
  (go
    (let [input (partition-or-time 1000 chan 500 5)]
      (while true
        (let [metrics (<! input)]
          (try
            (doseq [metric metrics]
              (let [formed (remove nil? (formatter rollups metric))]
                (doseq [f formed]
                  (>! insertch f))
                (doseq [p (distinct (map :path formed))]
                  (>! indexch p))))
            (catch Exception e
              (info "Exception for metric [" metrics "] : " e))))))))

(defn start
  "Start a tcp carbon listener"
  [{:keys [store carbon index]}]
  (let [indexch (path/channel-for index)
        insertch (store/channel-for store)
        chan (chan 100000)
        handler (format-processor chan indexch (:rollups carbon) insertch)]
    (info "starting carbon handler: " carbon)
    (tcp/start-tcp-server
     (merge carbon {:response-channel chan}))))
