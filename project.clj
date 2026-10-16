(defproject io.jepsen/history "0.1.9"
  :description "A library for working with Jepsen's histories of concurrent systems."
  :url "https://github.com/jepsen-io/history"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [; Logging
                 [org.clojure/tools.logging "1.3.1"]
                 ; Control flow, including fast single-threaded reductions
                 [dom-top "1.0.11"]
                 ; Fast reductions
                 [com.aphyr/tesser.core "1.0.10"]
                 ; Defining datatypes
                 [potemkin "0.4.9"]
                 ; Fast data structures
                 [com.aphyr/bifurcan-clj "0.1.6"]
                 ; Errors
                 [slingshot "0.12.2"]]
  :profiles {:dev {:dependencies
                   [[com.gfredericks/test.chuck "0.2.15"]
                    [org.clojure/clojure "1.12.6"]
                    [org.clojure/test.check "1.1.3"]
                    [org.slf4j/slf4j-simple "2.0.20"]
                    [cheshire "6.2.0"]
                    ]}}
  ; We need to write some Java code later that depends on these interfaces &
  ; defrecords.
  ;:aot [jepsen.history]
  :repl-options {:init-ns user}
  ; We deliberatly want a reasonable heap here, because our tests are going to
  ; blow it up.
  :jvm-opts ["-Xmx8g" "-server"
             ;"-XX:-OmitStackTraceInFastThrow"
;             "-agentpath:/home/aphyr/yourkit/bin/linux-x86-64/libyjpagent.so=disablestacktelemetry,exceptions=disable,delay=10000,usedmem=50"
             ]
  :test-selectors {:default (fn [m] (not (or (:slow m) (:perf m))))
                   :all         (fn [m] true)
                   :focus       :focus
                   :slow        :slow
                   :perf        :perf})
