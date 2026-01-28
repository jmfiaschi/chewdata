# [3.3.0](https://github.com/jmfiaschi/chewdata/compare/v3.2.0...v3.3.0) (2025-12-22)


### Features

* **updater:** add filter update/map/keys/values for an object. Migrate some function into filters. ([#66](https://github.com/jmfiaschi/chewdata/issues/66)) ([3f4525a](https://github.com/jmfiaschi/chewdata/commit/3f4525a95b143e81f1d8c3c36a360fae8469040d))
* **updater:** remove find/base64_encode/base64_decode from functions ([abf4bd4](https://github.com/jmfiaschi/chewdata/commit/abf4bd4f4c83b13e22f696e119da0fda4e723138))


# [3.2.0](https://github.com/jmfiaschi/chewdata/compare/v3.1.0...v3.2.0) (2025-12-15)


### Bug Fixes

* **curl:** bearer + header host + compatibility set_env with multi threading ([#65](https://github.com/jmfiaschi/chewdata/issues/65)) ([d3af728](https://github.com/jmfiaschi/chewdata/commit/d3af72839d13782fa643d107f4db3fcd6847fd52))


### Features

* **readme:** docs & force cicd ([a4b1025](https://github.com/jmfiaschi/chewdata/commit/a4b10259cf2838d17787cca818f16f1f22ad7c52))

# [3.1.0](https://github.com/jmfiaschi/chewdata/compare/v3.0.2...v3.1.0) (2025-05-30)


### Features

* **examples:** add test in examples to stabalize futur update ([#64](https://github.com/jmfiaschi/chewdata/issues/64)) ([fe5c00f](https://github.com/jmfiaschi/chewdata/commit/fe5c00fed08030e47ffe4141b49ef41434dfdfb7))

## [3.0.2](https://github.com/jmfiaschi/chewdata/compare/v3.0.1...v3.0.2) (2025-05-22)


### Bug Fixes

* **coverage:** start services ([#63](https://github.com/jmfiaschi/chewdata/issues/63)) ([9a0cdf2](https://github.com/jmfiaschi/chewdata/commit/9a0cdf25ad43bcec9be95c971f0f17548d8bc725))

## [3.0.1](https://github.com/jmfiaschi/chewdata/compare/v3.0.0...v3.0.1) (2025-05-22)


### Bug Fixes

* **project:** individual feature not built ([#62](https://github.com/jmfiaschi/chewdata/issues/62)) ([4cda7fb](https://github.com/jmfiaschi/chewdata/commit/4cda7fbe5922a4dc8e8e54110df7dd112cc10e93))
* **semantic:** not update changelog on other branches except main. ([08f5cdf](https://github.com/jmfiaschi/chewdata/commit/08f5cdff48a6797d3005d5ad9e975850d321ef0a))

# [3.0.0](https://github.com/jmfiaschi/chewdata/compare/v2.12.0...v3.0.0) (2025-05-21)


### Features

* **cargo:** update lib ([17577ba](https://github.com/jmfiaschi/chewdata/commit/17577ba189d22d6368b0f0d486700314cbebacba))
* **project:** remplace async_std by smol and surf by hyper ([#61](https://github.com/jmfiaschi/chewdata/issues/61)) ([2b729e4](https://github.com/jmfiaschi/chewdata/commit/2b729e423f3de7b1039fcbc5a9f300a1767ddf1c))


### BREAKING CHANGES

* **project:** remote surf cache and find better solution with http-cache and hyper
* feat(curl): add curl in bench
* feat(cicd): add podman install
* feat(project): replace IO by CLI
* feat(curl):use go-httpbin instead of httpbin
* feat(curl): manage http call retry when server close the connection
* chore(release): 2.12.0 [skip ci]

# [2.12.0](https://github.com/jmfiaschi/chewdata/compare/v2.11.0...v2.12.0) (2024-12-16)


### Features

* **project:** prefix variable environments with CHEWDATA ([#60](https://github.com/jmfiaschi/chewdata/issues/60)) ([19343e3](https://github.com/jmfiaschi/chewdata/commit/19343e3748e3f7f9a910d74f7f133b89d2bd3273))

# [2.11.0](https://github.com/jmfiaschi/chewdata/compare/v2.10.0...v2.11.0) (2024-05-20)


### Features

* **keycloak:** adapt test with new version of keycloak ([#59](https://github.com/jmfiaschi/chewdata/issues/59)) ([8653957](https://github.com/jmfiaschi/chewdata/commit/8653957ed61ed93225549a49c165d37c56e63df9))

# [2.10.0](https://github.com/jmfiaschi/chewdata/compare/v2.9.0...v2.10.0) (2024-03-08)


### Features

* **curl:** handle redirection ([#58](https://github.com/jmfiaschi/chewdata/issues/58)) ([0fc663f](https://github.com/jmfiaschi/chewdata/commit/0fc663f7820f5b2bd22fb13ff15007466dd99d19))

# [2.9.0](https://github.com/jmfiaschi/chewdata/compare/v2.8.1...v2.9.0) (2024-2-6)


### Features

* **json/jsonl:** write entry_path if define ([#57](https://github.com/jmfiaschi/chewdata/issues/57)) ([81fa6f6](https://github.com/jmfiaschi/chewdata/commit/81fa6f602197b51472db091a13f209a57aa9c4e6))

## [2.8.1](https://github.com/jmfiaschi/chewdata/compare/v2.8.0...v2.8.1) (2024-1-30)


### Performance Improvements

* **transform:** use tera::Context::from_value instead of tera_context.insert with Value serialization. ([#56](https://github.com/jmfiaschi/chewdata/issues/56)) ([acd1126](https://github.com/jmfiaschi/chewdata/commit/acd1126a412ca975238efdbf96a4addea06cfca0))

# [2.8.0](https://github.com/jmfiaschi/chewdata/compare/v2.7.0...v2.8.0) (2024-1-16)


### Features

* **referential:** group in a struct and add cache for none dynamic connector ([#55](https://github.com/jmfiaschi/chewdata/issues/55)) ([5be00d4](https://github.com/jmfiaschi/chewdata/commit/5be00d492b8aab9f5b7f1853929eff386d751e6e))

# [2.7.0](https://github.com/jmfiaschi/chewdata/compare/v2.6.0...v2.7.0) (2024-01-11)


### Features

* **local:** add cache for local connector ([#54](https://github.com/jmfiaschi/chewdata/issues/54)) ([8512b4c](https://github.com/jmfiaschi/chewdata/commit/8512b4cb696a17d6df214a6d1965c1ba342d5ad5))

# [2.6.0](https://github.com/jmfiaschi/chewdata/compare/v2.5.1...v2.6.0) (2024-01-11)


### Features

* **extract:** add merge_replace method for Value. Same as value.merge() but instead of append elements in a array, keep the same position and merge Value. ([#53](https://github.com/jmfiaschi/chewdata/issues/53)) ([d5e7d29](https://github.com/jmfiaschi/chewdata/commit/d5e7d29caf532c27321ad5d4e58a4ed947ee8698))

## [2.5.1](https://github.com/jmfiaschi/chewdata/compare/v2.5.0...v2.5.1) (2024-01-09)


### Bug Fixes

* **extract:** able to extract from a object a list of attribute. allow to use regex. ([#52](https://github.com/jmfiaschi/chewdata/issues/52)) ([bf5e744](https://github.com/jmfiaschi/chewdata/commit/bf5e7443986f00729e7c841c3e803b7cb9aa8871))

# [2.5.0](https://github.com/jmfiaschi/chewdata/compare/v2.4.0...v2.5.0) (2024-01-04)


### Features

* **updater:** add new filter/function `extract` for tera. Extraction attributes from an object or list of object. ([#51](https://github.com/jmfiaschi/chewdata/issues/51)) ([a719289](https://github.com/jmfiaschi/chewdata/commit/a71928910d8aaaee4a886ff10162691969d210d3))

# [2.4.0](https://github.com/jmfiaschi/chewdata/compare/v2.3.0...v2.4.0) (2024-01-03)


### Features

* **updater:** add new filter/function find for tera. ([#50](https://github.com/jmfiaschi/chewdata/issues/50)) ([5f457b1](https://github.com/jmfiaschi/chewdata/commit/5f457b16ec2ff58e875917ea72362178bf435ef7))

# [2.3.0](https://github.com/jmfiaschi/chewdata/compare/v2.2.1...v2.3.0) (2023-12-29)


### Features

* **updater:** add filter "find" and retreive all text match the pattern ([7321e89](https://github.com/jmfiaschi/chewdata/commit/7321e893e4c278f7b1b4ec4dbe12f243f9ded7fe))

## [2.2.1](https://github.com/jmfiaschi/chewdata/compare/v2.2.0...v2.2.1) (2023-12-20)


### Bug Fixes

* **curl:** set count_type optional and None by default ([f43fe7a](https://github.com/jmfiaschi/chewdata/commit/f43fe7acd036959ce864da6c19608937b47a16fe))

# [2.2.0](https://github.com/jmfiaschi/chewdata/compare/v2.1.0...v2.2.0) (2023-12-20)



### Features

* **document:** add byte format ([#46](https://github.com/jmfiaschi/chewdata/issues/46)) ([5f81b7e](https://github.com/jmfiaschi/chewdata/commit/5f81b7ef5cd11eada53e5e6e394d740d6b929bce))

# [2.1.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v2.0.0...v2.1.0-beta.1) (2023-12-19)


### Bug Fixes

* **bearer:** is_base64 specify if the token is already encoded or not. If not, it will be encoded. ([5a03e5a](https://github.com/jmfiaschi/chewdata/commit/5a03e5af6809d1633afb4b4e878e312bfbf598ad))
* **log:** log details not visible even with RUST_LOG=trace ([ae2fdf4](https://github.com/jmfiaschi/chewdata/commit/ae2fdf4b6bb6bdf2d5d7c4c697dc00668bfa1cf3))


### Features

* **document:** add byte format ([8a44cd7](https://github.com/jmfiaschi/chewdata/commit/8a44cd70bf724c8090b91c9f3e673c2119d5a1eb))

# [2.0.0](https://github.com/jmfiaschi/chewdata/compare/v1.17.0...v2.0.0) (2023-12-14)


### Features

* **connectors:** use OnceLock for lazy load client ([#45](https://github.com/jmfiaschi/chewdata/issues/45)) ([1cc3e56](https://github.com/jmfiaschi/chewdata/commit/1cc3e56006129da90de17dabe39bb4076399a5d0))


### BREAKING CHANGES

* **connectors:** for transformer step, remove step's input/output paramaters and use by default 'input'/'output' variable in the pattern action
* **connectors:** rename curl fields
* **connectors:** simplify autheticator and use it as a middleware
* **connectors:** remove description attributes and use hjson/yaml configuration formats
* fix(release): add missing dependency
* feat(updater): add function & filter env(name=key) or val ¦ env(name=key) ¦ ....
* feat(s3): upgrade version
* feat(minio): upgrade configuration
* feat(bucket): align bucket variables
* feat(bucket): Apply region and endpoint in this priority :
1 - from the config file
2 - from bucket env
3 - from aws env

# [2.0.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.18.0-beta.1...v2.0.0-beta.1) (2023-12-13)


### Bug Fixes

* **release:** add missing dependency ([773f5d0](https://github.com/jmfiaschi/chewdata/commit/773f5d06ce3a4bcd8d2fdb6b8765d365c68f2a61))


### Features

* **bucket:** align bucket variables ([2ed08b8](https://github.com/jmfiaschi/chewdata/commit/2ed08b8745fd2de197981509e2a1d4993f96d412))
* **bucket:** Apply region and endpoint in this priority : ([7f00f15](https://github.com/jmfiaschi/chewdata/commit/7f00f1504c5c63d75843b7d31a83ae44fa218d2d))
* **minio:** upgrade configuration ([01060e1](https://github.com/jmfiaschi/chewdata/commit/01060e131fdadc37aae70e703ef7c05c8314bdbb))
* **s3:** upgrade version ([814e583](https://github.com/jmfiaschi/chewdata/commit/814e5837e8db072c3b91ca806e239a7872b9484f))
* **updater:** add function & filter env(name=key) or val ¦ env(name=key) ¦ .... ([2ca673b](https://github.com/jmfiaschi/chewdata/commit/2ca673b32e49a937af30a527fce59f5015077797))
* upgrade version ([fccbbab](https://github.com/jmfiaschi/chewdata/commit/fccbbab912444d1d9a78c74296c90a8adcadadb5))


### BREAKING CHANGES

* for transformer step, remove step's input/output paramaters and use by default 'input'/'output' variable in the pattern action
* rename curl fields
* simplify autheticator and use it as a middleware
* remove description attributes and use hjson/yaml configuration formats

# [1.18.0-beta.2](https://github.com/jmfiaschi/chewdata/compare/v1.18.0-beta.1...v1.18.0-beta.2) (2023-11-12)


### Features

* **steps:** remove description attribute. Use hjson / Yaml format for your config file in order to add description ([29d3d5b](https://github.com/jmfiaschi/chewdata/commit/29d3d5b1595368e549d172c41745b5a27d95436e))

# [1.18.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.17.1-beta.1...v1.18.0-beta.1) (2023-09-10)


### Features

* **perf:** remove useless clone ([c8cdf9c](https://github.com/jmfiaschi/chewdata/commit/c8cdf9ceffa633629c99c4dde1c5a82ffb0fa4b8))

## [1.17.1-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.17.0...v1.17.1-beta.1) (2023-09-02)


### Bug Fixes

* **main:** accept hjson file extension ([00d924a](https://github.com/jmfiaschi/chewdata/commit/00d924a96d126ffb1a33722be7c7fe810cfa8ebc))

# [1.17.0](https://github.com/jmfiaschi/chewdata/compare/v1.16.0...v1.17.0) (2023-08-29)


### Features

* **configuration:** support hjson in the configuration by default ([#44](https://github.com/jmfiaschi/chewdata/issues/44)) ([5473c7c](https://github.com/jmfiaschi/chewdata/commit/5473c7cd20d057da16abe1a64d93ca4b0ca4c201))

# [1.17.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.16.0...v1.17.0-beta.1) (2023-08-28)


### Bug Fixes

* **json:** write an array generate data without terminator ([18632ef](https://github.com/jmfiaschi/chewdata/commit/18632ef824382f4239d867b008cd6ed99a18c381))
* **transformer:** if new result contain array, the transformer send each element from the array ([2658e96](https://github.com/jmfiaschi/chewdata/commit/2658e96dbfd78c3b61a05ce7a790d80718f03518))


### Features

* **configuration:** support hjson in the configuration by default ([800f6d1](https://github.com/jmfiaschi/chewdata/commit/800f6d1a1e76d95e9f963568bb323a947930277d))
* **local:** erase multi files with wildcard in the path. ([5d91ad9](https://github.com/jmfiaschi/chewdata/commit/5d91ad93ea1d3bc26c099dd62a1a73daa4a9237b))

# [1.16.0](https://github.com/jmfiaschi/chewdata/compare/v1.15.0...v1.16.0) (2023-08-23)


### Features

* **xml:** replace jxon by quick-xml ([#43](https://github.com/jmfiaschi/chewdata/issues/43)) ([40cd6d9](https://github.com/jmfiaschi/chewdata/commit/40cd6d9a94f8bb24944558ddfd468d9a6e30f264))

# [1.16.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.15.1-beta.1...v1.16.0-beta.1) (2023-08-23)


### Bug Fixes

* **xml:** add xml2json only if xml feature enable ([1fda46e](https://github.com/jmfiaschi/chewdata/commit/1fda46e89e0fdad2a7f446521ef35f89481f2ed3))


### Features

* **xml:** remove jxon library in order to use quick-xml ([1e9d24e](https://github.com/jmfiaschi/chewdata/commit/1e9d24e2ee557ffc3f03ea36c63a9fe086466fa8))

## [1.15.1-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.15.0...v1.15.1-beta.1) (2023-08-09)


### Bug Fixes

* **async-std:** use default option to avoid issue with --no-default-features ([45a5397](https://github.com/jmfiaschi/chewdata/commit/45a53972925292a242fb2e548ac2dbfa5d168cc8))
* **curl:** remove useless features ([27356b3](https://github.com/jmfiaschi/chewdata/commit/27356b3c7e0533a8028357837e61b0fe050e59b7))
* **features:** fix compile error when run features one by one ([f3ca965](https://github.com/jmfiaschi/chewdata/commit/f3ca9650a43d9c18f5df615498ed0b292f8700dc))
* **features:** specify features to test ([c7ae46a](https://github.com/jmfiaschi/chewdata/commit/c7ae46a59a00fc20c11c26780eb24060514a62e9))
* **local:** remove useless features by default ([d86ced2](https://github.com/jmfiaschi/chewdata/commit/d86ced2ad6e27d3721269ce3b97b8a3c769154e5))

# [1.15.0](https://github.com/jmfiaschi/chewdata/compare/v1.14.0...v1.15.0) (2023-08-09)


### Features

* **parquet:** upgrade versions and improve code ([#42](https://github.com/jmfiaschi/chewdata/issues/42)) ([819fa0d](https://github.com/jmfiaschi/chewdata/commit/819fa0d81f19a984a5c2ed4904d4c97d3859a262))
* **project:** improve the documentation and refacto ([#40](https://github.com/jmfiaschi/chewdata/issues/40)) ([ef44555](https://github.com/jmfiaschi/chewdata/commit/ef445559c4aacd66b315ae7b2f5a6eb789ab7e79))

# [1.15.0-beta.4](https://github.com/jmfiaschi/chewdata/compare/v1.15.0-beta.3...v1.15.0-beta.4) (2023-08-08)


### Bug Fixes

* **lint:** fix warnings ([410ced0](https://github.com/jmfiaschi/chewdata/commit/410ced0f682ef20fd46987b1ba29472159083387))
* **release:** apply --allow-dirty due to cargo.toml version change ([e473a21](https://github.com/jmfiaschi/chewdata/commit/e473a21b18e07a104146e59d7bc1f235bb43c6a2))


### Features

* **release:** speedup the CI ([e87b71c](https://github.com/jmfiaschi/chewdata/commit/e87b71c38d05fbd25049851f4132fd04e5773b2f))

# [1.15.0-beta.3](https://github.com/jmfiaschi/chewdata/compare/v1.15.0-beta.2...v1.15.0-beta.3) (2023-08-08)


### Bug Fixes

* **makefile:** set number of // jobs ([0c54668](https://github.com/jmfiaschi/chewdata/commit/0c54668960ee5357716725094577404a64009948))

# [1.15.0-beta.2](https://github.com/jmfiaschi/chewdata/compare/v1.15.0-beta.1...v1.15.0-beta.2) (2023-08-04)


### Bug Fixes

* **cargo:** upgrade version ([b6c5d1a](https://github.com/jmfiaschi/chewdata/commit/b6c5d1ada202be5a1885853f1e707ed4599841f4))


### Features

* **cargo:** upgrade version ([b53c145](https://github.com/jmfiaschi/chewdata/commit/b53c145fb4141571d8c4fc8d47ed286505483059))

# [1.15.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.14.0...v1.15.0-beta.1) (2023-08-03)


### Bug Fixes

* **main:** enable opentelemetry if apm feature declared ([78bb366](https://github.com/jmfiaschi/chewdata/commit/78bb3667ec575327061b29f3e29fa063bf4519ba))


### Features

* **cargo:** upgrade versions ([0dfa817](https://github.com/jmfiaschi/chewdata/commit/0dfa817af0728e301eace5c19f17f3f948d2bf4a))
* **cargo:** upgrade versions for toml & bucket ([792e2cf](https://github.com/jmfiaschi/chewdata/commit/792e2cf80daa7bb9d49383ab3bdf4f260cf693df))
* **example:** update tracing ([3b3a1da](https://github.com/jmfiaschi/chewdata/commit/3b3a1da806a89a913dbbe19bef276bcdf0391dfc))
* **parquet:** upgrade versions and improve ([f771f97](https://github.com/jmfiaschi/chewdata/commit/f771f97a6a2823c32d794ce7c01c8fde1b2f2ed9))
* **project:** improve the documentation and refacto ([#40](https://github.com/jmfiaschi/chewdata/issues/40)) ([ef44555](https://github.com/jmfiaschi/chewdata/commit/ef445559c4aacd66b315ae7b2f5a6eb789ab7e79))

# [1.15.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.14.0...v1.15.0-beta.1) (2023-07-29)


### Features

* **cargo:** upgrade versions ([0dfa817](https://github.com/jmfiaschi/chewdata/commit/0dfa817af0728e301eace5c19f17f3f948d2bf4a))
* **project:** improve the documentation and refacto ([#40](https://github.com/jmfiaschi/chewdata/issues/40)) ([ef44555](https://github.com/jmfiaschi/chewdata/commit/ef445559c4aacd66b315ae7b2f5a6eb789ab7e79))

# [1.15.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.14.0...v1.15.0-beta.1) (2023-07-23)


### Bug Fixes

* **tests:** change expected filesize unit ([76b077c](https://github.com/jmfiaschi/chewdata/commit/76b077c3ed11f30fe2fb3aa40fe65627e99e50b0))


### Features

* **auth:** improve the documentation ([cb2ea76](https://github.com/jmfiaschi/chewdata/commit/cb2ea76875f438c58781d3efedb2a9003a62cd80))
* **curl:** improve the documentation and code ([ec1e008](https://github.com/jmfiaschi/chewdata/commit/ec1e00857f83d75724b577ec4fe88edf1f57670c))
* **io&memory:** improve the documentation and refacto ([3f94517](https://github.com/jmfiaschi/chewdata/commit/3f945173cc53f2e75d8958ac3e8591300b1794ee))

# [1.14.0](https://github.com/jmfiaschi/chewdata/compare/v1.13.0...v1.14.0) (2023-02-08)


### Features

* add APM ([738caf1](https://github.com/jmfiaschi/chewdata/commit/738caf1dec709922e0806cd7c596df185b10e623))

# [1.14.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.13.0...v1.14.0-beta.1) (2023-02-04)


### Features

* **monitoring:** add APM feature ([1afbec3](https://github.com/jmfiaschi/chewdata/commit/1afbec3b7316f64fdb6129f761be359d345661c3))
* **monitoring:** add jaeger tracing ([56f444e](https://github.com/jmfiaschi/chewdata/commit/56f444ecc0fd52dcf011361674ec5a4432e3ad2a))

# [1.13.0](https://github.com/jmfiaschi/chewdata/compare/v1.12.2...v1.13.0) (2023-01-28)


### Features

* **rabbitmq:** support publish & consume ([#38](https://github.com/jmfiaschi/chewdata/issues/38)) ([9d708b6](https://github.com/jmfiaschi/chewdata/commit/9d708b60fcc62c696f0af753c5b4b11bc496f63b))

# [1.13.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.12.2...v1.13.0-beta.1) (2023-01-28)


### Bug Fixes

* **json:** write better array and handle empty data with [] ([ec11990](https://github.com/jmfiaschi/chewdata/commit/ec1199051959b23c530027542d1e6f0b2c5a0181))
* **json:** write better array and handle empty data with [] ([04199e4](https://github.com/jmfiaschi/chewdata/commit/04199e4f4525aa231985d8eeacf401d2eb7596a0))
* **json:** write better array and handle empty data with {} ([be998fa](https://github.com/jmfiaschi/chewdata/commit/be998faf98ede085c0d570e755ed0e73be5d6266))
* **project:** version ([5ec922f](https://github.com/jmfiaschi/chewdata/commit/5ec922f509471821cf9eeb47144430059caba55e))


### Features

* **base64:** add filters encode & decode ([c20bdf1](https://github.com/jmfiaschi/chewdata/commit/c20bdf1173e0c89a26e2c7a62b2ccf7a74c21060))
* **curl:** fetch can have a body for POST/PATCH/PUT ([d12e79b](https://github.com/jmfiaschi/chewdata/commit/d12e79b88d6340ba92e2c56abc77f10ccd0199b0))
* **rabbitmq:**  publish and consume data ([fe9b5a7](https://github.com/jmfiaschi/chewdata/commit/fe9b5a75e9439650d71e3f2ae153421fbeb6abd7))
* **tracing:** add tracing-log and  display lib logs ([701af48](https://github.com/jmfiaschi/chewdata/commit/701af48db05ad39e29931674add612c40706fb3a))

# [1.13.0-beta.3](https://github.com/jmfiaschi/chewdata/compare/v1.13.0-beta.2...v1.13.0-beta.3) (2023-01-15)


### Bug Fixes

* **ci:** add toolchain for semantic-release ([691636c](https://github.com/jmfiaschi/chewdata/commit/691636cb91ef823042dd7af8da1ecafb9ed8c436))
* **ci:** upgrade node version for semantic-release ([ef1c786](https://github.com/jmfiaschi/chewdata/commit/ef1c786a45725967e1ddf15362ca57afe5b930fa))
* **lint:** add eq in the derive ([6efa0e4](https://github.com/jmfiaschi/chewdata/commit/6efa0e464843975cb90aaeb84d6c2b2c8055632e))
* **lint:** replace consecutive ([ce1f985](https://github.com/jmfiaschi/chewdata/commit/ce1f985907913592dd8a63554af84374521343d3))
* **psql:** query sanitized and add example ([8d3cc47](https://github.com/jmfiaschi/chewdata/commit/8d3cc478d2190f43ba8728e121926b7e5e181494))
* **psql:** query sanitized and add example ([5168f6f](https://github.com/jmfiaschi/chewdata/commit/5168f6facfd2e74f64f50dde85d7ae547e5b6a8a))


### Features

* **examples:** add example for psql ([829bf69](https://github.com/jmfiaschi/chewdata/commit/829bf69795a03062980a10758747f48e0e17213c))
* **examples:** add example for psql ([5133b97](https://github.com/jmfiaschi/chewdata/commit/5133b97b145b939593eebfd4f59d9c35fa3024fe))
* **jwt:** with Keycloak ([fa1fb69](https://github.com/jmfiaschi/chewdata/commit/fa1fb6930b361b39376d0df99248ad037e7de746))
* **jwt:** with Keycloak ([1343fff](https://github.com/jmfiaschi/chewdata/commit/1343ffffa9a3687c02c2cd672dd075c9be4060e1))


### Performance Improvements

* **send & fetch:** replace &box(T) by  &T ([4c158f8](https://github.com/jmfiaschi/chewdata/commit/4c158f839e2652e6261fa410e65af59d11fa3d83))

# [1.13.0-beta.2](https://github.com/jmfiaschi/chewdata/compare/v1.13.0-beta.1...v1.13.0-beta.2) (2022-11-27)


### Bug Fixes

* **lint:** add eq in the derive ([941d5c1](https://github.com/jmfiaschi/chewdata/commit/941d5c1736aacaa95d48b3a9f339fea2d9290bfd))
* **lint:** replace consecutive ([f939566](https://github.com/jmfiaschi/chewdata/commit/f939566f63da624356d82bda92617fdad35ce407))


### Performance Improvements

* **send & fetch:** replace &box(T) by  &T ([adf8257](https://github.com/jmfiaschi/chewdata/commit/adf8257afd67d2bc51928690f2c4c2012df760c3))

# [1.13.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.12.2...v1.13.0-beta.1) (2022-11-25)


### Bug Fixes

* **.env:** remove CARGO_INCREMENTAL=1 ([345ed48](https://github.com/jmfiaschi/chewdata/commit/345ed48b3c64588130a11290112ab47c6b288171))
* **cargo:** use only postgres for sqlx ([be24f70](https://github.com/jmfiaschi/chewdata/commit/be24f703e3466dcb20c3ae6e3f79296fd13ab6c2))
* **curl:** parameters can have value paginator.next for cursor paginator ([bdc2849](https://github.com/jmfiaschi/chewdata/commit/bdc2849924f26c3b2db95872eec8e008fe0738a3))
* **lint:** add cache ([b22e6a4](https://github.com/jmfiaschi/chewdata/commit/b22e6a4997291c975ed212a6d71845e8d5358afb))
* **linter:** update files ([a416438](https://github.com/jmfiaschi/chewdata/commit/a4164388ed258ad2d10f89619df91278d82dcb91))
* **makefile:** remove cargo clean during the build phase and use the cache. win time during the compilation ([48014b0](https://github.com/jmfiaschi/chewdata/commit/48014b0a1f1144e6d7819bd4677bd46e0c973761))
* **release:** remove semantic-release-rust ([2b26a3a](https://github.com/jmfiaschi/chewdata/commit/2b26a3a0f8d26b789ca70d86bfb0915b4d9ccca6))
* **release:** verify condition ([55a306b](https://github.com/jmfiaschi/chewdata/commit/55a306b97295e357673e3772e08fb80df9a0d9f4))
* **release:** verify condition ([a712beb](https://github.com/jmfiaschi/chewdata/commit/a712bebfef329ccc277bc64a893a66b0a252fe37))
* **release:** verify condition ([11944f8](https://github.com/jmfiaschi/chewdata/commit/11944f8965c1676c817b544d2e38e9634567da8b))
* **window:** replace sh script by sql sript and avoid issue with text format ([2426dfe](https://github.com/jmfiaschi/chewdata/commit/2426dfe94edd16a096ce817c0b45fbdd4a048780))


### Features

* **cargo:** optimize dependencies ([9a1886b](https://github.com/jmfiaschi/chewdata/commit/9a1886b5ccdb9083a17d55b9157fff99e5add0c5))
* **cargo:** optimize dependencies ([b62b99d](https://github.com/jmfiaschi/chewdata/commit/b62b99d67e005dfa62457aad277871463d492f5f))
* **cargo:** ugrade criterion ([aa050ad](https://github.com/jmfiaschi/chewdata/commit/aa050ad739c9088812470810ed28efbf228c12af))
* **cargo:** upgrade version ([37d3d12](https://github.com/jmfiaschi/chewdata/commit/37d3d12b12e066284d624ad99e0b67620d69ac59))
* **cicd:** upgrade versions ([0969db0](https://github.com/jmfiaschi/chewdata/commit/0969db055d43404216d487067abe25128b93d4b4))
* **clap:** upgrade version ([d08d517](https://github.com/jmfiaschi/chewdata/commit/d08d51729de4030cfde7717e17154ea22ca9e904))
* **csv:** flatten object & array ([8ced5e6](https://github.com/jmfiaschi/chewdata/commit/8ced5e6d510442062e3a8c2a5f14d17a2e82fc59))
* **makefile:** add setup command to install cargo extensions ([6a09f6e](https://github.com/jmfiaschi/chewdata/commit/6a09f6ee3694355ba30f080877fa7e3030a7dfb0))
* **makefile:** add setup command to install cargo extensions ([9114262](https://github.com/jmfiaschi/chewdata/commit/9114262201984e9cabd265cd159058fd610560ae))
* **release:** replace semantic-release-rust by standard cli ([f7c5609](https://github.com/jmfiaschi/chewdata/commit/f7c560960d13e00309b6d958f8220ba3ef4a7175))
* **serde_yaml:** upgrade version ([ec36d46](https://github.com/jmfiaschi/chewdata/commit/ec36d46095a62ec0f0cedfb7caf77fd0e6bc824e))

# [1.13.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.12.2...v1.13.0-beta.1) (2022-11-25)


### Bug Fixes

* **.env:** remove CARGO_INCREMENTAL=1 ([345ed48](https://github.com/jmfiaschi/chewdata/commit/345ed48b3c64588130a11290112ab47c6b288171))
* **cargo:** use only postgres for sqlx ([be24f70](https://github.com/jmfiaschi/chewdata/commit/be24f703e3466dcb20c3ae6e3f79296fd13ab6c2))
* **curl:** parameters can have value paginator.next for cursor paginator ([bdc2849](https://github.com/jmfiaschi/chewdata/commit/bdc2849924f26c3b2db95872eec8e008fe0738a3))
* **linter:** add cache ([b22e6a4](https://github.com/jmfiaschi/chewdata/commit/b22e6a4997291c975ed212a6d71845e8d5358afb))
* **linter:** update files ([a416438](https://github.com/jmfiaschi/chewdata/commit/a4164388ed258ad2d10f89619df91278d82dcb91))
* **makefile:** remove cargo clean during the build phase and use the cache. win time during the compilation ([48014b0](https://github.com/jmfiaschi/chewdata/commit/48014b0a1f1144e6d7819bd4677bd46e0c973761))
* **release:** remove semantic-release-rust ([2b26a3a](https://github.com/jmfiaschi/chewdata/commit/2b26a3a0f8d26b789ca70d86bfb0915b4d9ccca6))
* **window:** replace sh script by sql script and avoid issue with text format ([2426dfe](https://github.com/jmfiaschi/chewdata/commit/2426dfe94edd16a096ce817c0b45fbdd4a048780))


### Features

* **cargo:** optimize dependencies ([9a1886b](https://github.com/jmfiaschi/chewdata/commit/9a1886b5ccdb9083a17d55b9157fff99e5add0c5))
* **cargo:** ugrade criterion ([aa050ad](https://github.com/jmfiaschi/chewdata/commit/aa050ad739c9088812470810ed28efbf228c12af))
* **cargo:** upgrade version ([37d3d12](https://github.com/jmfiaschi/chewdata/commit/37d3d12b12e066284d624ad99e0b67620d69ac59))
* **cicd:** upgrade version ([0969db0](https://github.com/jmfiaschi/chewdata/commit/0969db055d43404216d487067abe25128b93d4b4))
* **clap:** upgrade version ([d08d517](https://github.com/jmfiaschi/chewdata/commit/d08d51729de4030cfde7717e17154ea22ca9e904))
* **csv:** flatten object & array ([8ced5e6](https://github.com/jmfiaschi/chewdata/commit/8ced5e6d510442062e3a8c2a5f14d17a2e82fc59))
* **makefile:** add setup command to install cargo extensions ([6a09f6e](https://github.com/jmfiaschi/chewdata/commit/6a09f6ee3694355ba30f080877fa7e3030a7dfb0))
* **release:** replace semantic-release-rust by standard cli ([f7c5609](https://github.com/jmfiaschi/chewdata/commit/f7c560960d13e00309b6d958f8220ba3ef4a7175))
* **serde_yaml:** upgrade version ([ec36d46](https://github.com/jmfiaschi/chewdata/commit/ec36d46095a62ec0f0cedfb7caf77fd0e6bc824e))

## [1.12.2](https://github.com/jmfiaschi/chewdata/compare/v1.12.1...v1.12.2) (2022-07-29)


### Bug Fixes

* **makefile:**  fix run command ([#34](https://github.com/jmfiaschi/chewdata/issues/34)) ([94dbf01](https://github.com/jmfiaschi/chewdata/commit/94dbf01e41e236f45f3f70b7bf03d585c13785eb))

## [1.12.2-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.12.1...v1.12.2-beta.1) (2022-07-28)


### Bug Fixes

* **makefile:**  fix run command ([55e1da5](https://github.com/jmfiaschi/chewdata/commit/55e1da5bbdbb802da56a74280f189497aca29ae6))

## [1.12.1](https://github.com/jmfiaschi/chewdata/compare/v1.12.0...v1.12.1) (2022-07-27)


### Bug Fixes

* **io:** the stream return only one connector ([#33](https://github.com/jmfiaschi/chewdata/issues/33)) ([e8fb7b1](https://github.com/jmfiaschi/chewdata/commit/e8fb7b1e56bce52680e8b969b685c0bbf2856cd7))

# [1.13.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.12.1-beta.1...v1.13.0-beta.1) (2022-07-26)


### Bug Fixes

* **bench:** document read ([5e9056f](https://github.com/jmfiaschi/chewdata/commit/5e9056f942cd8f455993af82ef62c75ae178cc81))
* **cargo:** upgrade ([18cc073](https://github.com/jmfiaschi/chewdata/commit/18cc0736a1241c7bc8d7d1d63699e07a1480aded))
* **curl:** auth ([9c01414](https://github.com/jmfiaschi/chewdata/commit/9c01414fff89a6d3dd152617f9ca6979a1355c80))
* **examples:** improve the examples ([608a4a7](https://github.com/jmfiaschi/chewdata/commit/608a4a7f2744a138f1aa33a64e4d5daf01338aa3))
* **faker:** upgrade & fix ([cfd2f05](https://github.com/jmfiaschi/chewdata/commit/cfd2f0582bbf1d7d94a8f7bee2b6cc0a50d450c5))
* **makefile:** clean ([6af87b1](https://github.com/jmfiaschi/chewdata/commit/6af87b11da15fe808bc604d2330c01f078fb0df6))
* **parquet:** function write ([37d5b33](https://github.com/jmfiaschi/chewdata/commit/37d5b336a3d56930497af67a92d5173f0a57af1f))
* **parquet:** upgrade & fix ([c814e4f](https://github.com/jmfiaschi/chewdata/commit/c814e4f8aeab51c76b2f92e8e449ff00f68deb34))


### Features

* **feature:** refacto feature names ([77df6b2](https://github.com/jmfiaschi/chewdata/commit/77df6b2e1419459dc3778e90b78dc1fe52bacef1))
* **psql/msql/sqlite/sqlite:** simplify the code for the futur connectors ([53f6fed](https://github.com/jmfiaschi/chewdata/commit/53f6fed78f2b5f5f31ec215f39f51e63725a1b11))
* **psql/msql/sqlite/sqlite:** simplify the code for the futur connectors ([d8386f2](https://github.com/jmfiaschi/chewdata/commit/d8386f2eccbfc856357e3cf985f237660e57c328))
* **psql:** add psql connector feature ([a1cdb2b](https://github.com/jmfiaschi/chewdata/commit/a1cdb2b94f2c23203729692d638684485ffad866))

## [1.12.1-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.12.0...v1.12.1-beta.1) (2022-05-22)

### Bug Fixes

* **io:** the stream return only one connector ([19cc5ab](https://github.com/jmfiaschi/chewdata/commit/19cc5ab54fd7241aa4e438ff1871442926cad4d4))

# [1.12.0](https://github.com/jmfiaschi/chewdata/compare/v1.11.0...v1.12.0) (2022-05-21)

### Features

* **asw_sdk:** replace rusoto and remove hardcoding credentials ([#32](https://github.com/jmfiaschi/chewdata/issues/32)) ([d97e074](https://github.com/jmfiaschi/chewdata/commit/d97e0743776cc6c10a20e435426c9db11c894371))
* **parquet:** handle parquet document ([#23](https://github.com/jmfiaschi/chewdata/issues/23)) ([0839281](https://github.com/jmfiaschi/chewdata/commit/0839281840df4d16aba2c7a955e33530830eef42))

# [1.12.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.11.0...v1.12.0-beta.1) (2022-05-21)

### Bug Fixes

* **bucket:** is_empty doc test ([e6503f6](https://github.com/jmfiaschi/chewdata/commit/e6503f6bbf72e46c3c94a9bc751294edadccfd42))
* **bucket:** remove hardcoding credentials, use profiles or env var instead ([f309c92](https://github.com/jmfiaschi/chewdata/commit/f309c92d86e9cac5b455e9916a85071650e96a13))
* **lint:** clean code ([9275bc5](https://github.com/jmfiaschi/chewdata/commit/9275bc507f5984e3fb6b61c19ab0a2990b84ea4d))
* **reader:** fix test ([251a2f6](https://github.com/jmfiaschi/chewdata/commit/251a2f68f0254deb9823b3bd90d41259f0f3b163))
* **test:** there is no reactor running, must be called from the context of a Tokio 1.x runtime ([5114086](https://github.com/jmfiaschi/chewdata/commit/5114086f45c155e6cec37f89284d1f73ac754aac))
* **tokio:** replace tokio macro by async_std ([393b65a](https://github.com/jmfiaschi/chewdata/commit/393b65a9ef0422ef7b4ac4cee5febf08fedb7cd0))

### Features

* **asw_sdk:** replace rusoto and remove hardcoding credentials ([31545bf](https://github.com/jmfiaschi/chewdata/commit/31545bf6670e70373bbac13bb942991ab976ee61))
* **cargo:** replace crossbeam by async-channel ([836d7a6](https://github.com/jmfiaschi/chewdata/commit/836d7a6b2f42bb719848d63078a61903f39a5b31))
* **cargo:** upgrade uuid ([22e397d](https://github.com/jmfiaschi/chewdata/commit/22e397d5e323505fb83903a896c239342b4da7c7))
* **parquet:** handle parquet document ([#23](https://github.com/jmfiaschi/chewdata/issues/23)) ([0839281](https://github.com/jmfiaschi/chewdata/commit/0839281840df4d16aba2c7a955e33530830eef42))

## [1.11.1-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.11.0...v1.11.1-beta.1) (2022-04-04)

### Bug Fixes

* **release:** build release bin and publish it ([156112b](https://github.com/jmfiaschi/chewdata/commit/156112b01b931ff2c231b3b88b27a63c265b88ec))

## [1.11.1-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.11.0...v1.11.1-beta.1) (2022-02-03)

### Bug Fixes

* **release:** build release bin and publish it ([156112b](https://github.com/jmfiaschi/chewdata/commit/156112b01b931ff2c231b3b88b27a63c265b88ec))

## [1.11.1-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.11.0...v1.11.1-beta.1) (2022-02-02)

### Bug Fixes

* **release:** build release bin and publish it ([156112b](https://github.com/jmfiaschi/chewdata/commit/156112b01b931ff2c231b3b88b27a63c265b88ec))

## [1.11.1-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.11.0...v1.11.1-beta.1) (2022-02-01)

### Bug Fixes

* **linter:** fix some warning ([fa0b7ef](https://github.com/jmfiaschi/chewdata/commit/fa0b7ef8b60c886bd6e490bfb4e30f3fe1ad341f))

## [1.11.1-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.11.0...v1.11.1-beta.1) (2022-01-30)

### Bug Fixes

* **linter:** fix some warning ([fa0b7ef](https://github.com/jmfiaschi/chewdata/commit/fa0b7ef8b60c886bd6e490bfb4e30f3fe1ad341f))

# [1.11.0](https://github.com/jmfiaschi/chewdata/compare/v1.10.0...v1.11.0) (2022-01-29)

### Features

* **reader:** use offset/cursor paginator with iterative/concurrency mode ([#22](https://github.com/jmfiaschi/chewdata/issues/22)) ([f8b2cad](https://github.com/jmfiaschi/chewdata/commit/f8b2cadfb11f0b42fdd69e92b2669d3fdbdff3fd))

# [1.11.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.10.0...v1.11.0-beta.1) (2022-01-29)

### Bug Fixes

* **clap:** last version ([b4ad056](https://github.com/jmfiaschi/chewdata/commit/b4ad056c4144bdc521935029717339ff3a63ef6a))
* **log:** messages ([921e226](https://github.com/jmfiaschi/chewdata/commit/921e226f82f432e5ecded7a5e4c8fbee6303031b))
* **step_context:** add step_context to avoid variable names collision ([c05e80b](https://github.com/jmfiaschi/chewdata/commit/c05e80ba2a7e5131ccd77695f1b097a9170619c2))
* **test:** mongodb ([c190888](https://github.com/jmfiaschi/chewdata/commit/c1908883c6b66589081ce92bac49405028400c26))
* **test:** no blocking sender/receiver ([0f7ed10](https://github.com/jmfiaschi/chewdata/commit/0f7ed1047cfb30e9b571c3a3af4e1731528a459c))

### Features

* **connector:** replace next_page by a stream() for the paginator ([e16ce3d](https://github.com/jmfiaschi/chewdata/commit/e16ce3d6822540b87321ae6cd2851ce62c63a0ee))
* **curl:** add offset/cursor base paginator ([ebf85b7](https://github.com/jmfiaschi/chewdata/commit/ebf85b7b766af1c0c00631266af6a1c6e859660f))
* **example:** add sub command ([b1ded94](https://github.com/jmfiaschi/chewdata/commit/b1ded94d73d46b11d0f970f49bc6baef3547bc71))
* **mongo&curl:** default value ([b6dad4c](https://github.com/jmfiaschi/chewdata/commit/b6dad4cbb7cc7d5f27cfc6d11b6ac11780ce2c5d))
* **parallel:** Possibility to read  in parallel data with offset pagination and  multi files with same structure of data ([2b2009d](https://github.com/jmfiaschi/chewdata/commit/2b2009d206fc92950642936a5f58326e6170e24e))
* **quality:** forbid unsafe code ([da1c317](https://github.com/jmfiaschi/chewdata/commit/da1c317a9c7f2fff0127c3a3fe4b4f7250e5867a))
* **step:** add wait/sleep field. The step wait/sleep is the pipe is not ready without blocking the thread ([ceef80b](https://github.com/jmfiaschi/chewdata/commit/ceef80b6ad2622b0225503a10b528f54d197a519))
* **step:** replace alias by name to identify a step ([443fd43](https://github.com/jmfiaschi/chewdata/commit/443fd4392e325006ab48753777fc03b0ae93cf69))
* **validator:** Add a validator step ([09bbeb7](https://github.com/jmfiaschi/chewdata/commit/09bbeb7ff44daba445027777e374826c8cffead3))
* **validator:** Add tests and docs ([f1986df](https://github.com/jmfiaschi/chewdata/commit/f1986df515531641469d02fe5e8a27115aacca1c))

# [1.10.0-beta.5](https://github.com/jmfiaschi/chewdata/compare/v1.10.0-beta.4...v1.10.0-beta.5) (2022-01-19)

### Bug Fixes

* **test:** mongodb ([c268cdf](https://github.com/jmfiaschi/chewdata/commit/c268cdf8bee4d5cb32c91f71da60753c3eb303a8))

### Features

* **example:** add sub command ([b1ded94](https://github.com/jmfiaschi/chewdata/commit/b1ded94d73d46b11d0f970f49bc6baef3547bc71))

# [1.10.0-beta.4](https://github.com/jmfiaschi/chewdata/compare/v1.10.0-beta.3...v1.10.0-beta.4) (2022-01-16)

### Bug Fixes

* **clap:** last version ([b4ad056](https://github.com/jmfiaschi/chewdata/commit/b4ad056c4144bdc521935029717339ff3a63ef6a))
* **log:** messages ([921e226](https://github.com/jmfiaschi/chewdata/commit/921e226f82f432e5ecded7a5e4c8fbee6303031b))
* **test:** no blocking sender/receiver ([0f7ed10](https://github.com/jmfiaschi/chewdata/commit/0f7ed1047cfb30e9b571c3a3af4e1731528a459c))

### Features

* **connector:** replace next_page by a stream() for the paginator ([e16ce3d](https://github.com/jmfiaschi/chewdata/commit/e16ce3d6822540b87321ae6cd2851ce62c63a0ee))
* **curl:** add offset/cursor base paginator ([ebf85b7](https://github.com/jmfiaschi/chewdata/commit/ebf85b7b766af1c0c00631266af6a1c6e859660f))
* **mongo&curl:** default value ([b6dad4c](https://github.com/jmfiaschi/chewdata/commit/b6dad4cbb7cc7d5f27cfc6d11b6ac11780ce2c5d))
* **parallel:** Possibility to read  in parallel data with offset pagination and  multi files with same structure of data ([2b2009d](https://github.com/jmfiaschi/chewdata/commit/2b2009d206fc92950642936a5f58326e6170e24e))
* **quality:** forbid unsafe code ([da1c317](https://github.com/jmfiaschi/chewdata/commit/da1c317a9c7f2fff0127c3a3fe4b4f7250e5867a))
* **step:** add wait/sleep field. The step wait/sleep is the pipe is not ready without blocking the thread ([ceef80b](https://github.com/jmfiaschi/chewdata/commit/ceef80b6ad2622b0225503a10b528f54d197a519))

# [1.10.0-beta.3](https://github.com/jmfiaschi/chewdata/compare/v1.10.0-beta.2...v1.10.0-beta.3) (2021-12-26)

### Bug Fixes

* **step_context:** add step_context to avoid variable names collision ([#20](https://github.com/jmfiaschi/chewdata/issues/20)) ([77469bb](https://github.com/jmfiaschi/chewdata/commit/77469bb9e72bd05120a08bfcc88be43a9341b7f4))

### Features

* **step:** replace alias by name to identify a step ([443fd43](https://github.com/jmfiaschi/chewdata/commit/443fd4392e325006ab48753777fc03b0ae93cf69))
* **validator:** Add a validator step ([09bbeb7](https://github.com/jmfiaschi/chewdata/commit/09bbeb7ff44daba445027777e374826c8cffead3))
* **validator:** Add tests and docs ([f1986df](https://github.com/jmfiaschi/chewdata/commit/f1986df515531641469d02fe5e8a27115aacca1c))

# [1.10.0-beta.2](https://github.com/jmfiaschi/chewdata/compare/v1.10.0-beta.1...v1.10.0-beta.2) (2021-12-26)

### Features

* **step:** replace alias by name to identify a step ([55e2fdc](https://github.com/jmfiaschi/chewdata/commit/55e2fdc1ea66bd3670f4116d36d81c3dfc93a1c2))

# [1.10.0-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.9.0...v1.10.0-beta.1) (2021-12-26)

### Features

* **validator:** Add a validator step ([697aecc](https://github.com/jmfiaschi/chewdata/commit/697aecc36698c07ec1953dcd0add3877aa3ba3f0))
* **validator:** Add tests and docs ([4107e41](https://github.com/jmfiaschi/chewdata/commit/4107e41f792416e16153e7a5ecb02a177f2f97cd))

## [1.9.1](https://github.com/jmfiaschi/chewdata/compare/v1.9.0...v1.9.1) (2021-12-20)

### Bug Fixes

* **step_context:** add step_context to avoid variable names collision ([#20](https://github.com/jmfiaschi/chewdata/issues/20)) ([77469bb](https://github.com/jmfiaschi/chewdata/commit/77469bb9e72bd05120a08bfcc88be43a9341b7f4))

## [1.9.1-beta.1](https://github.com/jmfiaschi/chewdata/compare/v1.9.0...v1.9.1-beta.1) (2021-12-20)

### Bug Fixes

* **step_context:** add step_context to avoid variable names collision ([c05e80b](https://github.com/jmfiaschi/chewdata/commit/c05e80ba2a7e5131ccd77695f1b097a9170619c2))

# [1.9.0](https://github.com/jmfiaschi/chewdata/compare/v1.8.4...v1.9.0) (2021-12-19)

### Features

* **tera:** remove set_env function ([25d52c5](https://github.com/jmfiaschi/chewdata/commit/25d52c556abcea926322922b554f2e612391a38c))

# [1.9.0-beta.3](https://github.com/jmfiaschi/chewdata/compare/v1.9.0-beta.2...v1.9.0-beta.3) (2021-12-19)

### Bug Fixes

* **ci:** use specific key ([1770fd0](https://github.com/jmfiaschi/chewdata/commit/1770fd02c842388ba9efc884ef2ccedfbe2c9e07))

# [1.9.0-beta.2](https://github.com/jmfiaschi/chewdata/compare/v1.9.0-beta.1...v1.9.0-beta.2) (2021-12-19)

### Bug Fixes

* **eraser:** clean files event with empty data in input ([9c8ccae](https://github.com/jmfiaschi/chewdata/commit/9c8ccaee9d1a059091da52e8b6fc53b6b0706f8c))

## [1.8.4](https://github.com/jmfiaschi/chewdata/compare/v1.8.3...v1.8.4) (2021-12-06)

### Bug Fixes

* **bucket:** use the DefaultCredentialsProvider by default ([#18](https://github.com/jmfiaschi/chewdata/issues/18)) ([0cd6b09](https://github.com/jmfiaschi/chewdata/commit/0cd6b09e5f8cf8202350faa81404b4f21b70b252))

## [1.8.3](https://github.com/jmfiaschi/chewdata/compare/v1.8.2...v1.8.3) (2021-12-03)

### Bug Fixes

* **erase:** can clear data in the document before and after a step ([#17](https://github.com/jmfiaschi/chewdata/issues/17)) ([b638908](https://github.com/jmfiaschi/chewdata/commit/b638908b9ed5325bb3d3c6da85d2d585a632b86c))

## [1.8.2](https://github.com/jmfiaschi/chewdata/compare/v1.8.1...v1.8.2) (2021-11-30)

### Bug Fixes

* **eraser:** erase data in static connector before to share new data ([#16](https://github.com/jmfiaschi/chewdata/issues/16)) ([5f0a565](https://github.com/jmfiaschi/chewdata/commit/5f0a565c853c9f46c3ce573ef509ad29824309d6))

## [1.8.1](https://github.com/jmfiaschi/chewdata/compare/v1.8.0...v1.8.1) (2021-11-30)

### Bug Fixes

* **transformer:** give more detail on the tera errors ([#15](https://github.com/jmfiaschi/chewdata/issues/15)) ([0f415b4](https://github.com/jmfiaschi/chewdata/commit/0f415b4b5d03b7979facc964c456086b51a41466))

# [1.8.0](https://github.com/jmfiaschi/chewdata/compare/v1.7.0...v1.8.0) (2021-11-29)

### Features

* **tera:** add object search by path ([#14](https://github.com/jmfiaschi/chewdata/issues/14)) ([4accb4e](https://github.com/jmfiaschi/chewdata/commit/4accb4e46530d1e6a80804fd6a639fbe2bc66fa3))

# [1.7.0](https://github.com/jmfiaschi/chewdata/compare/v1.6.0...v1.7.0) (2021-11-28)

### Features

* **steps:** remove the field wait ([#13](https://github.com/jmfiaschi/chewdata/issues/13)) ([70afab8](https://github.com/jmfiaschi/chewdata/commit/70afab8deb53938614b88bd2b951e95acc0d2159))

# [1.6.0](https://github.com/jmfiaschi/chewdata/compare/v1.5.1...v1.6.0) (2021-11-07)

### Features

* **external_input_and_output:** give the possibility to inject an input_receiver and output_sender ([#12](https://github.com/jmfiaschi/chewdata/issues/12)) ([c23edfa](https://github.com/jmfiaschi/chewdata/commit/c23edfac3616a62d3cea2108d5149acb6b06279f))

## [1.5.1](https://github.com/jmfiaschi/chewdata/compare/v1.5.0...v1.5.1) (2021-10-10)

### Bug Fixes

* **dependency:** key value in error ([60c21be](https://github.com/jmfiaschi/chewdata/commit/60c21be191c3b73123023d1cc889969ea10bb5a2))

# [1.5.0](https://github.com/jmfiaschi/chewdata/compare/v1.4.0...v1.5.0) (2021-10-09)

### Features

* **logs:** replace slog by tracing and multiqueue2 by crossbeam ([#11](https://github.com/jmfiaschi/chewdata/issues/11)) ([e3a15e8](https://github.com/jmfiaschi/chewdata/commit/e3a15e8fb8f0af142df5c899e8741920a7db4f4d))

# [1.4.0](https://github.com/jmfiaschi/chewdata/compare/v1.3.1...v1.4.0) (2021-10-03)

### Features

* **io:** update curl / xml / logs / auth ([#10](https://github.com/jmfiaschi/chewdata/issues/10)) ([8e702ae](https://github.com/jmfiaschi/chewdata/commit/8e702ae9f6163f28d600ccd0d40e0274a0b01656))

## [1.3.1](https://github.com/jmfiaschi/chewdata/compare/v1.3.0...v1.3.1) (2021-09-24)

### Bug Fixes

* **xml:** fix transform string to scalar in the xml document ([583f8a0](https://github.com/jmfiaschi/chewdata/commit/583f8a0c94ef0764661a507b8a9a2cb7cae048ac))

# [1.3.0](https://github.com/jmfiaschi/chewdata/compare/v1.2.0...v1.3.0) (2021-09-24)

### Features

* **project:** externalize the documentation and fix xml issues ([365ea40](https://github.com/jmfiaschi/chewdata/commit/365ea40f7b18036b4a25a9b683b2fb6a1603da63))

# [1.2.0](https://github.com/jmfiaschi/chewdata/compare/v1.1.0...v1.2.0) (2021-09-17)

### Features

* **project:** update bucket_select and documentation ([#6](https://github.com/jmfiaschi/chewdata/issues/6)) ([98c9acb](https://github.com/jmfiaschi/chewdata/commit/98c9acb34cc48dc89026fde4b388368afc360fe1))

# [1.1.0](https://github.com/jmfiaschi/chewdata/compare/v1.0.0...v1.1.0) (2021-08-29)

### Bug Fixes

* **CD:** install semantic-release/exec ([cb2c206](https://github.com/jmfiaschi/chewdata/commit/cb2c20622615c57e93d49e4e4f44e2113f1dd27f))

### Features

* **cd:** add  semantic-release-rust ([85dcbc2](https://github.com/jmfiaschi/chewdata/commit/85dcbc231542969dbf9353b563f0eee1cabf5df5))
* **project:** refacto the code ([6c1717a](https://github.com/jmfiaschi/chewdata/commit/6c1717ae21ffc1ec28e318c02482b81a798558d3))

# 1.0.0 (2020-12-17)

### Features

* **codecoverage:** add codecov feature ([3e82950](https://github.com/jmfiaschi/chewdata/commit/3e82950b03a55c8a39162748264f9ba81e044de4))
* **project:** init project ([d0b1344](https://github.com/jmfiaschi/chewdata/commit/d0b1344a9fefa8ed14e2e0f1910605cbf339012d))
* **semantic_release:** add feature ([7f928e2](https://github.com/jmfiaschi/chewdata/commit/7f928e23bd6be8423ec8a47dc531375e4f0f1027))
