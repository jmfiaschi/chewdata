# [1.9.0](https://github.com/jmfiaschi/chewdata/compare/v1.8.4...v1.9.0) (2021-12-19)


### Features

* **tera:** remove set_env function ([25d52c5](https://github.com/jmfiaschi/chewdata/commit/25d52c556abcea926322922b554f2e612391a38c))

# [1.9.0-beta.3](https://github.com/jmfiaschi/chewdata/compare/v1.9.0-beta.2...v1.9.0-beta.3) (2021-12-19)


### Bug Fixes

* **ci:** use specific key ([1770fd0](https://github.com/jmfiaschi/chewdata/commit/1770fd02c842388ba9efc884ef2ccedfbe2c9e07))
* **ci:** use specific key ([d97c7e3](https://github.com/jmfiaschi/chewdata/commit/d97c7e3068c1141b1fa07ca75a38f290df64e7d5))

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
