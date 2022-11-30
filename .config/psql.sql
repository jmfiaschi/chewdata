CREATE TABLE read (
    "number" INT NULL,
    "group" INT NULL,
    "string" TEXT NULL,
    "long-string" TEXT NULL,
    "boolean" BOOLEAN NULL,
    "special_char" CHAR(10) NULL,
    "rename_this" TEXT NULL,
    "date" DATE NULL DEFAULT CURRENT_DATE,
    "filesize" INT NULL,
    "round" FLOAT NULL,
    "url" TEXT NULL,
    "list_to_sort" TEXT NULL,
    "code" TEXT NULL,
    "remove_field" TEXT NULL,
    "null" TEXT null,
    "array" jsonb,
    "object" jsonb
);

INSERT INTO read VALUES (1, 1561, 'value to test 5416', 'Long val\nto test', FALSE, 'é','field must be renamed','2019-12-31',1000000,10.156,'?search=test me','A,B,C','value_to_map','field to remove',null, '[1,2]', '{"field":"value"}');
INSERT INTO read VALUES (2, 5448, 'test', 'Long val\nto test', FALSE, 'é',NULL,'2022-12-31',5000000,12.222,'?search=test me 2','A,5,C','value_to_map2','field to remove2',null, '[1,2]', '{"field":"value"}');

CREATE TABLE erase (
    "data" TEXT NOT NULL
);

CREATE TABLE send (
    "number" INT NULL,
    "group" INT NULL,
    "string" TEXT NULL,
    "long-string" TEXT NULL,
    "boolean" BOOLEAN NULL,
    "special_char" CHAR(10) NULL,
    "rename_this" TEXT NULL,
    "date" DATE NULL DEFAULT CURRENT_DATE,
    "filesize" INT NULL,
    "round" FLOAT NULL,
    "url" TEXT NULL,
    "list_to_sort" TEXT NULL,
    "code" TEXT NULL,
    "remove_field" TEXT NULL
);

INSERT INTO send VALUES (1, 1561, 'value to test 5416', 'Long val\nto test', FALSE, 'é','field must be renamed','2019-12-31',1000000,10.156,'?search=test me','A,B,C','value_to_map','field to remove');

CREATE TABLE send_update (
    "number" INT NULL,
    "group" INT NULL,
    "string" TEXT NULL,
    "long-string" TEXT NULL,
    "boolean" BOOLEAN NULL,
    "special_char" CHAR(10) NULL,
    "rename_this" TEXT NULL,
    "date" DATE NULL DEFAULT CURRENT_DATE,
    "filesize" INT NULL,
    "round" FLOAT NULL,
    "url" TEXT NULL,
    "list_to_sort" TEXT NULL,
    "code" TEXT NULL,
    "remove_field" TEXT NULL
);

INSERT INTO send_update VALUES (1, 1561, 'value to test 5416', 'Long val\nto test', FALSE, 'é','field must be renamed','2019-12-31',1000000,10.156,'?search=test me','A,B,C','value_to_map','field to remove');

CREATE TABLE send_with_key (
    "number" INT PRIMARY KEY,
    "group" INT NULL,
    "string" TEXT NULL,
    "long-string" TEXT NULL,
    "boolean" BOOL NULL,
    "special_char" CHAR(10) NULL,
    "rename_this" TEXT NULL,
    "date" DATE NULL DEFAULT CURRENT_DATE,
    "filesize" INT NULL,
    "round" FLOAT NULL,
    "url" TEXT NULL,
    "list_to_sort" TEXT NULL,
    "code" TEXT NULL,
    "remove_field" TEXT NULL,
    "null" TEXT null,
    "array" jsonb,
    "object" jsonb
);

INSERT INTO send_with_key VALUES (1, 1561, 'value to test 5416', 'Long val\nto test', FALSE, 'é','field must be renamed','2019-12-31',1000000,10.156,'?search=test me','A,B,C','value_to_map','field to remove',null, '[1,2]', '{"field":"value"}');

CREATE SCHEMA examples;

CREATE TABLE examples.simple_insert (
    "number" INT PRIMARY KEY,
    "group" INT NULL,
    "string" TEXT NULL,
    "long-string" TEXT NULL,
    "boolean" BOOL NULL,
    "special_char" CHAR(10) NULL,
    "rename_this" TEXT NULL,
    "date" DATE NULL DEFAULT CURRENT_DATE,
    "filesize" INT NULL,
    "round" FLOAT NULL,
    "url" TEXT NULL,
    "list_to_sort" TEXT NULL,
    "code" TEXT NULL,
    "remove_field" TEXT NULL,
    "null" TEXT null,
    "array" jsonb,
    "object" jsonb
);
