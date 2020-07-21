#standardSQL

-- Users
DROP TABLE IF EXISTS PP.U_USERS;
CREATE TABLE PP.U_USERS (
    username                    STRING NOT NULL
    , password                  BYTES
    , role                      STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP
);

-- Roles
DROP TABLE IF EXISTS PP.U_ROLES;
CREATE TABLE PP.U_ROLES (
    role                        STRING
    , description               STRING
);

-- Sessions
DROP TABLE IF EXISTS PP.U_SESSIONS;
CREATE TABLE PP.U_SESSIONS (
    id                          STRING NOT NULL
    , username                  STRING NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP NOT NULL
);
