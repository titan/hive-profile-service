CREATE TABLE users(
    id uuid PRIMARY KEY,
    openid char(28) NOT NULL,
    password char(32),
    name char(64),
    gender char(4),
    identity_no char(18),
    phone char(11),
    nickname char(64),
    portrait char(1024)
);
