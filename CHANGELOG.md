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
