# Contributing to sqlite4clj

TODO: flesh this out when needed


## Development

We assume you have already installed, and have made available in your PATH:

- [babashka][bb] - used as the project's task runner
- [clj-kondo][kondo] - clojure static analyzer and linter

Run the linter:

```
bb lint
```

Run the test suite:

```
bb test
```


Run the test suite in watch mode

```
bb test --watch
```

Run a specific test

```
bb test --focus sqlite4clj.functions-test/removing-functions
```

See [kaocha docs][kaocha] for more tips.

[bb]: https://babashka.org/
[kaocha]: https://cljdoc.org/d/lambdaisland/kaocha/
[kondo]: https://github.com/clj-kondo/clj-kondo
