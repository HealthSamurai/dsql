  (:require [cider.nrepl :as ci]
            [nrepl.middleware :as mw]
            [nrepl.server :as nrepl-server]
            [refactor-nrepl.middleware :as refactor]))

(defn restart-cyto []
  (cytocheck.core/dev-restart)
  (cytocheck.core/dev-stop))

(defn restart-mawd []
  (prn "Restarting mawd.")
  (mawd.core/dev-restart))

(defn delete-recursively [f]
  (when (.isDirectory f)
    (doseq [c (.listFiles f)]
      (delete-recursively c)))
  (io/delete-file f))

(defn restart-shadow-clean []
  (shadow.server/stop!)
  (try (-> (shadow.config/get-build :app)
           (get-in [:dev :output-dir])
           (io/file)
           (delete-recursively))
       (delete-recursively (io/file ".shadow-cljs"))
       (catch Exception _))
  (shadow.server/start!)
  (shadow/watch :app))

(defn restart-shadow []
  (shadow.server/stop!)
  (delete-recursively (io/file ".shadow-cljs"))
  (shadow.server/start!)
  (shadow/watch :app))

(defn restart-fw []
  (restart-shadow-clean))

(defn reload-ns []
  (repl/set-refresh-dirs "src" "src-c" "src-ui")
  (repl/refresh))

(defn cljs-repl []
  (shadow/repl :app))

(defn save-port-file
  [port]
  (let [port-file (io/file ".nrepl-port")]
    (.deleteOnExit port-file)
    (spit port-file port)))

(def handler
  (let [mws
        (->> ci/cider-middleware
             (map resolve)
             (concat nrepl-server/default-middlewares))
        stack
        (->> (conj mws #'refactor/wrap-refactor)
             mw/linearize-middleware-stack
             reverse
             (apply comp))]
    (stack nrepl-server/unknown-op)))

(defn start [& _]
  (let [port 7004]
    (nrepl-server/start-server :port port :handler handler)
    (save-port-file 7004)
    (println (str "\nnRepl started on " port ".\n "))))
