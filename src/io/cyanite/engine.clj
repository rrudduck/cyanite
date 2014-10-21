(ns io.cyanite.engine
  (:require [clojure.cor.async :refer [chan go <!]]))

(defn to-seconds
  "Takes a string containing a duration like 13s, 4h etc. and
   converts it to seconds"
  [s]
  (let [[_ value unit] (re-matches #"^([0-9]+)([a-z])$" s)
        quantity (Integer/valueOf value)]
    (case unit
      "s" quantity
      "m" (* 60 quantity)
      "h" (* 60 60 quantity)
      "d" (* 24 60 60 quantity)
      "w" (* 7 24 60 60 quantity)
      "y" (* 365 24 60 60 quantity)
      (throw (ex-info (str "unknown rollup unit: " unit) {})))))

(defn from-shorthand-rollup
  "Converts an individual rollup to a {:rollup :period :ttl} tri"
  [rollup]
  (if (string? rollup)
    (let [[rollup-string retention-string] (split rollup #":" 2)
          rollup-secs (to-seconds rollup-string)
          retention-secs (to-seconds retention-string)]
      {:rollup rollup-secs
       :period (/ retention-secs rollup-secs)
       :ttl (* rollup-secs (/ retention-secs rollup-secs))})
    rollup))

(defn rollup-to
  "Enhance a rollup definition with a function to compute
   the rollup of a point"
  [{:keys [rollup] :as rollup-def}]
  (assoc rollup-def :rollup-to #(-> % (quot rollup) (* rollup))))

(defn engine
  [{:keys [buffer-size rollups]}]
  (let [rollups (mapv (comp rollup-to from-shorthand-rollup) rollups)
        inch    (chan buffer-size)
        outch   (chan buffer-size)]
    (go
      (loop [msg (<! inch)]
        ;; magic happens here
        (recur (<! inch))))
    {:in  inch
     :out outch}))
