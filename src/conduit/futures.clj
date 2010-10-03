(ns conduit.futures
  (:use
     clojure.test
     conduit.core)
  (:import [java.util.concurrent TimeoutException TimeUnit]))

(defn a-future 
  "define a proc that runs a given proc in a future with an optional timeout"
  ([p]
   (assoc p
          :reply (fn fut-reply [x]
                   (let [e (atom nil)
                         result @(future
                                   (try
                                     (let [[new-x new-f] ((:reply p) x)]
                                       [new-x fut-reply])
                                     (catch Exception fe
                                       (reset! e fe))))]
                     (if @e
                       (throw @e)
                       result)))
          :no-reply (fn fut-no-reply [x]
                      (future
                        ((:reply p) x))
                      [[] fut-no-reply])
          :scatter-gather (fn fut-sg-reply [x]
                            (let [e (atom nil)
                                  fut (future
                                        (try
                                          (let [[new-x new-f] ((:reply p) x)]
                                            [new-x fut-sg-reply])
                                          (catch Exception fe
                                            (reset! e fe))))]
                              (fn []
                                (let [result @fut]
                                  (if @e
                                    (throw @e)
                                    result)))))))
  ([msecs p]
   (assoc p
          :reply (fn fut-reply [x]
                   (let [e (atom nil)
                         result (.get
                                  (future
                                    (try
                                      (let [[new-x new-f] ((:reply p) x)]
                                        [new-x fut-reply])
                                      (catch Exception fe
                                        (reset! e fe))))
                                  msecs TimeUnit/MILLISECONDS)]
                     (if @e
                       (throw @e)
                       result)))
          :no-reply (fn fut-no-reply [x]
                      (future
                        ((:reply p) x))
                      [[] fut-no-reply])
          :scatter-gather (fn fut-sg-reply [x]
                            (let [e (atom nil)
                                  fut (future
                                        (try
                                          (let [[new-x new-f] ((:reply p) x)]
                                            [new-x fut-sg-reply])
                                          (catch Exception fe
                                            (reset! e fe))))]
                              (fn []
                                (let [result (.get fut msecs TimeUnit/MILLISECONDS)]
                                  (if @e
                                    (throw @e)
                                    result))))))))
