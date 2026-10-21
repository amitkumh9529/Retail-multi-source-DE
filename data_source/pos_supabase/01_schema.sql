-- RetailPulse POS schema (Supabase / PostgreSQL)
create extension if not exists pgcrypto;

drop table if exists pos_transactions cascade;
drop table if exists products cascade;
drop table if exists stores cascade;

create table stores (
    store_id        int primary key,
    store_name      text not null,
    region          text not null,
    opened_on       date not null
);

create table products (
    product_id      text primary key,
    product_name    text not null,
    category        text not null,
    list_price      numeric(10,2) not null check (list_price >= 0)
);

create table pos_transactions (
    transaction_id  bigserial primary key,
    store_id        int not null references stores(store_id),
    product_id      text not null references products(product_id),
    customer_id     text,
    quantity        int not null check (quantity <> 0),
    unit_price      numeric(10,2) not null check (unit_price >= 0),
    txn_ts          timestamptz not null,
    updated_at      timestamptz not null,
    payment_method  text not null
);

create index pos_transactions_updated_at_idx on pos_transactions (updated_at);
create index pos_transactions_store_idx     on pos_transactions (store_id);
create index pos_transactions_txn_ts_idx    on pos_transactions (txn_ts);