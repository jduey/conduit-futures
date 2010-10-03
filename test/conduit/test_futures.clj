(ns conduit.test-futures
  (:use conduit.futures :reload-all)
  (:use
     clojure.test
     conduit.core)
  (:import [java.util.concurrent TimeoutException]))

(deftest test-reply-timeout
         (is (thrown? TimeoutException
                      (conduit-map (a-comp
                                     (a-future 100 
                                             (a-arr (fn [_] (Thread/sleep 200))))
                                     pass-through)
                                   [nil])))
         
         (is (= TimeoutException
                (try
                  (conduit-map (a-comp
                                     (a-all
                                       pass-through
                                       (a-future 100 
                                                 (a-arr (fn [_] (Thread/sleep 200)))))
                                     pass-through)
                                   [nil])
                  (catch java.lang.RuntimeException e
                    (.getClass
                      (.getCause e)))))))

(deftest test-future-reply
         (is (= [1 2 3 4]
                (conduit-map (a-comp
                               (a-future (a-arr inc))
                               pass-through)
                             (range 4))))

         (is (= "from inside future"
                (try
                  (conduit-map (a-comp
                                 (a-future
                                   (a-arr (fn [_]
                                            (throw (Exception. "from inside future")))))
                                 pass-through)
                               [nil])
                  (catch Exception e
                          (.getMessage e))))))

(deftest test-future-no-reply
         (is (= [4 3 2 1]
               (let [result (atom [])]
                 (conduit-map (a-future
                                (a-arr (fn [x]
                                         (Thread/sleep (- 100 (* 10 x)))
                                         (swap! result conj (inc x)))))
                              (range 4))
                 (Thread/sleep 300)
                 @result)))

         (is (= [4 2 1]
               (let [result (atom [])]
                 (conduit-map (a-future
                                (a-arr (fn [x]
                                         (when (= x 2)
                                           (throw (Exception. "bogus exception")))
                                         (Thread/sleep (- 100 (* 10 x)))
                                         (swap! result conj (inc x)))))
                              (range 4))
                 (Thread/sleep 300)
                 @result))))

(deftest test-future-scatter-gather
         (is (= [[0 1] [1 2] [2 3] [3 4]]
                (conduit-map (a-comp
                               (a-all
                                 pass-through
                                 (a-future (a-arr inc)))
                               pass-through)
                             (range 4))))
         
         (is (= "from inside future"
                (try
                  (conduit-map (a-comp
                                 (a-all
                                   pass-through
                                   (a-future
                                     (a-arr (fn [_]
                                              (throw (Exception. "from inside future"))))))
                                 pass-through)
                               [nil])
                  (catch Exception e
                    (.getMessage
                      (.getCause e))))))
         
         (is (= [4 3 2 1]
                (let [result (atom [])]
                  (conduit-map (a-all
                                 pass-through
                                 (a-future
                                   (a-arr (fn [x]
                                            (Thread/sleep (- 100 (* 10 x)))
                                            (swap! result conj (inc x))))))
                               (range 4))
                  (Thread/sleep 300)
                  @result))))

(run-tests)
