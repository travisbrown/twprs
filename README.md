# Twitter profile tools for Rust

[![Rust build status](https://img.shields.io/github/workflow/status/travisbrown/twprs/rust-ci.svg?label=rust)](https://github.com/travisbrown/twprs/actions)
[![Coverage status](https://img.shields.io/codecov/c/github/travisbrown/twprs/main.svg)](https://codecov.io/github/travisbrown/twprs)

This project contains tools for working with JSON user objects representing Twitter profiles from the Twitter API,
including the following:

* Parsing tools for working with the [Twitter Stream Grab][twitter-stream-grab] from the [Internet Archive][internet-archive].
* An [Avro][avro] schema and Avro conversion tools for efficient storage of user objects.

## License

This project is licensed under the Mozilla Public License, version 2.0. See the LICENSE file for details.

Please note that we are only using the MPL in order to support use from ✨[cancel-culture]✨, which is currently
published under the MPL. Future versions of both ✨[cancel-culture]✨ and this project are likely to be published
under the [Anti-Capitalist Software License][acsl].

[acsl]: https://anticapitalist.software/
[avro]: https://avro.apache.org/
[cancel-culture]: https://github.com/travisbrown/cancel-culture
[internet-archive]: https://archive.org/details/twitterstream
[twitter-stream-grab]: https://archive.org/details/twitterstream