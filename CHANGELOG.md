## 0.0.5 (April 27, 2015)

BACKWARDS INCOMPATIBILITIES:
* Moved from com.charlieknudsen to smartthings package
* Changed maven identifier to smarthings:konsumer
* Default try count set to 1 (no retries by default)

IMPROVEMENTS:
* Added retries for simple message consumer

BUG FIXES:
* Fix bug where retries were not happening on ThreadedMessageConsumer

## 0.0.4 (April 24, 2015)

IMPROVEMENTS:
* Removed the guava dependency since it caused dependency issues upstream.

## 0.0.3 (April 16, 2015)

BACKWARDS INCOMPATIBILITIES:

FEATURES:
* Added ability to have kafka listening thread process messages and not require consumer thread pool.

IMPROVEMENTS:
* Added findbugs.
* Added a circle-ci build file.

BUG FIXES:

## 0.0.2 (April 5, 2015)

BUG FIXES:

* Fix issue in published pom file causing compile dependencies to show up as runtime.

## 0.0.1 (April 5, 2015)

* Initial release
