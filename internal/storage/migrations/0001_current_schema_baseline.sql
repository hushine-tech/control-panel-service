
SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: market_data_coverage_segments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS market_data_coverage_segments (
    segment_id bigint NOT NULL,
    exchange text NOT NULL,
    market text NOT NULL,
    kind text NOT NULL,
    symbol text NOT NULL,
    "interval" text NOT NULL,
    year integer NOT NULL,
    segment_start_at timestamp with time zone NOT NULL,
    segment_end_at timestamp with time zone NOT NULL,
    row_count bigint NOT NULL,
    source text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT market_data_coverage_segments_check CHECK ((segment_end_at > segment_start_at)),
    CONSTRAINT market_data_coverage_segments_exchange_check CHECK ((exchange = ANY (ARRAY['binance'::text, 'okx'::text]))),
    CONSTRAINT market_data_coverage_segments_interval_check CHECK (("interval" <> ''::text)),
    CONSTRAINT market_data_coverage_segments_kind_check CHECK ((kind = 'kline'::text)),
    CONSTRAINT market_data_coverage_segments_market_check CHECK ((market = ANY (ARRAY['spot'::text, 'futures'::text]))),
    CONSTRAINT market_data_coverage_segments_row_count_check CHECK ((row_count > 0)),
    CONSTRAINT market_data_coverage_segments_source_check CHECK ((source <> ''::text)),
    CONSTRAINT market_data_coverage_segments_symbol_check CHECK ((symbol <> ''::text)),
    CONSTRAINT market_data_coverage_segments_year_check CHECK ((year >= 1970))
);


--
-- Name: market_data_coverage_segments_segment_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS market_data_coverage_segments_segment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: market_data_coverage_segments_segment_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE market_data_coverage_segments_segment_id_seq OWNED BY market_data_coverage_segments.segment_id;


--
-- Name: market_data_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS market_data_requests (
    request_id bigint NOT NULL,
    user_id bigint NOT NULL,
    portfolio_id bigint,
    exchange text NOT NULL,
    market text NOT NULL,
    kind text NOT NULL,
    symbol text NOT NULL,
    "interval" text NOT NULL,
    needs_live_delivery boolean DEFAULT true NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    stream_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    cancelled_at timestamp with time zone,
    CONSTRAINT market_data_requests_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'active'::text, 'cancelled'::text])))
);


--
-- Name: market_data_requests_request_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS market_data_requests_request_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: market_data_requests_request_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE market_data_requests_request_id_seq OWNED BY market_data_requests.request_id;


--
-- Name: market_data_history_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS market_data_history_requests (
    request_id bigint DEFAULT nextval('market_data_requests_request_id_seq'::regclass) NOT NULL,
    user_id bigint NOT NULL,
    portfolio_id bigint,
    exchange text NOT NULL,
    market text NOT NULL,
    kind text NOT NULL,
    symbol text NOT NULL,
    "interval" text NOT NULL,
    requested_start_at timestamp with time zone NOT NULL,
    requested_end_at timestamp with time zone NOT NULL,
    covered_start_at timestamp with time zone,
    covered_end_at timestamp with time zone,
    last_error text,
    status text DEFAULT 'pending'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    cancelled_at timestamp with time zone,
    CONSTRAINT market_data_history_requests_check CHECK ((requested_end_at > requested_start_at)),
    CONSTRAINT market_data_history_requests_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'running'::text, 'verifying'::text, 'ready'::text, 'error'::text, 'cancelled'::text])))
);


--
-- Name: market_data_leases; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS market_data_leases (
    lease_id bigint NOT NULL,
    session_id text NOT NULL,
    strategy_id bigint,
    portfolio_id bigint,
    stream_id bigint NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    last_heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    released_at timestamp with time zone
);


--
-- Name: market_data_leases_lease_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS market_data_leases_lease_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: market_data_leases_lease_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE market_data_leases_lease_id_seq OWNED BY market_data_leases.lease_id;


--
-- Name: market_data_streams; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS market_data_streams (
    stream_id bigint NOT NULL,
    exchange text NOT NULL,
    market text NOT NULL,
    kind text NOT NULL,
    symbol text NOT NULL,
    "interval" text NOT NULL,
    desired_state text DEFAULT 'running'::text NOT NULL,
    actual_state text DEFAULT 'pending'::text NOT NULL,
    effective_live_delivery boolean DEFAULT false NOT NULL,
    last_data_at timestamp with time zone,
    last_error text,
    last_reconciled_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT market_data_streams_actual_state_check CHECK ((actual_state = ANY (ARRAY['pending'::text, 'starting'::text, 'running'::text, 'draining'::text, 'stopped'::text, 'error'::text]))),
    CONSTRAINT market_data_streams_desired_state_check CHECK ((desired_state = ANY (ARRAY['running'::text, 'stopped'::text])))
);


--
-- Name: market_data_streams_stream_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS market_data_streams_stream_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: market_data_streams_stream_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE market_data_streams_stream_id_seq OWNED BY market_data_streams.stream_id;


--
-- Name: market_data_writer_leases; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS market_data_writer_leases (
    lease_id text NOT NULL,
    exchange text NOT NULL,
    market text NOT NULL,
    kind text NOT NULL,
    symbol text NOT NULL,
    "interval" text DEFAULT ''::text NOT NULL,
    year integer NOT NULL,
    owner_instance_id text NOT NULL,
    scraper_instance_id text DEFAULT ''::text NOT NULL,
    collector_id text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    acquired_at timestamp with time zone DEFAULT now() NOT NULL,
    last_heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    released_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT market_data_writer_leases_collector_id_check CHECK ((collector_id <> ''::text)),
    CONSTRAINT market_data_writer_leases_exchange_check CHECK ((exchange = ANY (ARRAY['binance'::text, 'okx'::text]))),
    CONSTRAINT market_data_writer_leases_kind_check CHECK ((kind = ANY (ARRAY['kline'::text, 'funding_rate'::text, 'open_interest'::text, 'orderbook'::text]))),
    CONSTRAINT market_data_writer_leases_market_check CHECK ((market = ANY (ARRAY['spot'::text, 'futures'::text]))),
    CONSTRAINT market_data_writer_leases_owner_instance_id_check CHECK ((owner_instance_id <> ''::text)),
    CONSTRAINT market_data_writer_leases_status_check CHECK ((status = ANY (ARRAY['active'::text, 'released'::text, 'expired'::text, 'stolen'::text]))),
    CONSTRAINT market_data_writer_leases_year_check CHECK ((year >= 1970))
);


--
-- Name: runtime_admission_failures; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS runtime_admission_failures (
    admission_failure_id bigint NOT NULL,
    user_id bigint DEFAULT 0 NOT NULL,
    credential_key_id text DEFAULT ''::text NOT NULL,
    requested_runtime_id text DEFAULT ''::text NOT NULL,
    requested_name text DEFAULT ''::text NOT NULL,
    source text DEFAULT ''::text NOT NULL,
    role text DEFAULT ''::text NOT NULL,
    failure_code text DEFAULT ''::text NOT NULL,
    reason text NOT NULL,
    consumed_runtime_id text DEFAULT ''::text NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    attempt_count integer DEFAULT 1 NOT NULL,
    CONSTRAINT runtime_admission_failures_attempt_count_check CHECK ((attempt_count > 0)),
    CONSTRAINT runtime_admission_failures_reason_check CHECK ((reason <> ''::text))
);


--
-- Name: runtime_admission_failures_admission_failure_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS runtime_admission_failures_admission_failure_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: runtime_admission_failures_admission_failure_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE runtime_admission_failures_admission_failure_id_seq OWNED BY runtime_admission_failures.admission_failure_id;


--
-- Name: runtime_channel_leases; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS runtime_channel_leases (
    runtime_id text NOT NULL,
    user_id bigint NOT NULL,
    credential_key_id text NOT NULL,
    lease_hash text NOT NULL,
    issued_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    last_used_at timestamp with time zone,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT runtime_channel_leases_check CHECK ((expires_at > issued_at)),
    CONSTRAINT runtime_channel_leases_lease_hash_check CHECK ((lease_hash <> ''::text)),
    CONSTRAINT runtime_channel_leases_runtime_id_check CHECK ((runtime_id <> ''::text))
);


--
-- Name: runtime_commands; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS runtime_commands (
    command_id text NOT NULL,
    user_id bigint NOT NULL,
    runtime_id text NOT NULL,
    session_id text DEFAULT ''::text NOT NULL,
    idempotency_key text DEFAULT ''::text NOT NULL,
    command_type text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    deadline_at timestamp with time zone NOT NULL,
    sent_at timestamp with time zone,
    acked_at timestamp with time zone,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    cancelled_at timestamp with time zone,
    attempt_count integer DEFAULT 0 NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    result jsonb DEFAULT '{}'::jsonb NOT NULL,
    failure_reason text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT runtime_commands_command_type_check CHECK ((command_type = ANY (ARRAY['start_session'::text, 'stop_session'::text, 'finish_session'::text, 'shutdown_runtime'::text, 'status_patch'::text]))),
    CONSTRAINT runtime_commands_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'sent'::text, 'acked'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'timed_out'::text, 'cancelled'::text])))
);


--
-- Name: runtime_credentials; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS runtime_credentials (
    key_id text NOT NULL,
    user_id bigint NOT NULL,
    public_key_pem text NOT NULL,
    label text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    last_used_at timestamp with time zone,
    role text DEFAULT 'executor'::text NOT NULL,
    downloaded_at timestamp with time zone,
    consumed_at timestamp with time zone,
    consumed_runtime_id text DEFAULT ''::text NOT NULL,
    expires_at timestamp with time zone,
    hosted_internal boolean DEFAULT false NOT NULL,
    client_cert_pem text DEFAULT ''::text NOT NULL,
    client_cert_fingerprint text DEFAULT ''::text NOT NULL,
    client_cert_expires_at timestamp with time zone,
    issuer text DEFAULT 'user'::text NOT NULL,
    CONSTRAINT ck_runtime_credentials_issuer CHECK ((issuer = ANY (ARRAY['user'::text, 'hosted_internal'::text, 'bare_debug'::text]))),
    CONSTRAINT ck_runtime_credentials_lifecycle CHECK ((((status = 'active'::text) AND (downloaded_at IS NULL) AND (consumed_at IS NULL) AND (consumed_runtime_id = ''::text)) OR ((status = 'downloaded'::text) AND (downloaded_at IS NOT NULL) AND (consumed_at IS NULL) AND (consumed_runtime_id = ''::text)) OR ((status = 'consumed'::text) AND (consumed_at IS NOT NULL) AND (consumed_runtime_id <> ''::text)) OR ((status = 'revoked'::text) AND (revoked_at IS NOT NULL) AND (((consumed_at IS NULL) AND (consumed_runtime_id = ''::text)) OR ((consumed_at IS NOT NULL) AND (consumed_runtime_id <> ''::text)))) OR ((status = 'expired'::text) AND (expires_at IS NOT NULL) AND (consumed_at IS NULL) AND (consumed_runtime_id = ''::text)))),
    CONSTRAINT ck_runtime_credentials_role CHECK ((role = ANY (ARRAY['executor'::text, 'debugger'::text]))),
    CONSTRAINT ck_runtime_credentials_status CHECK ((status = ANY (ARRAY['active'::text, 'downloaded'::text, 'consumed'::text, 'revoked'::text, 'expired'::text])))
);


--
-- Name: runtime_debug_datasets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS runtime_debug_datasets (
    dataset_id text NOT NULL,
    user_id bigint NOT NULL,
    portfolio_id bigint NOT NULL,
    runtime_id text NOT NULL,
    market text NOT NULL,
    symbol text NOT NULL,
    "interval" text NOT NULL,
    start_at timestamp with time zone NOT NULL,
    end_at timestamp with time zone NOT NULL,
    bar_count bigint NOT NULL,
    coverage_status text NOT NULL,
    state text NOT NULL,
    last_error text DEFAULT ''::text NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT runtime_debug_datasets_bar_count_check CHECK ((bar_count >= 0)),
    CONSTRAINT runtime_debug_datasets_coverage_status_check CHECK ((coverage_status <> ''::text)),
    CONSTRAINT runtime_debug_datasets_interval_check CHECK (("interval" <> ''::text)),
    CONSTRAINT runtime_debug_datasets_market_check CHECK ((market = ANY (ARRAY['spot'::text, 'futures'::text]))),
    CONSTRAINT runtime_debug_datasets_state_check CHECK ((state = ANY (ARRAY['active'::text, 'lost'::text, 'failed'::text]))),
    CONSTRAINT runtime_debug_datasets_symbol_check CHECK ((symbol <> ''::text))
);


--
-- Name: runtime_registry; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS runtime_registry (
    runtime_id text NOT NULL,
    user_id bigint,
    name text NOT NULL,
    source text NOT NULL,
    endpoint_host text NOT NULL,
    grpc_port integer NOT NULL,
    debug_port integer,
    capabilities jsonb DEFAULT '[]'::jsonb NOT NULL,
    resource_profile text NOT NULL,
    version text DEFAULT ''::text NOT NULL,
    status text NOT NULL,
    token_hash text DEFAULT ''::text NOT NULL,
    paired_at timestamp with time zone,
    heartbeat_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    credential_key_id text,
    started_at timestamp with time zone,
    ended_at timestamp with time zone,
    ended_reason text DEFAULT ''::text NOT NULL,
    role text DEFAULT 'executor'::text NOT NULL,
    last_heartbeat_at timestamp with time zone,
    connection_owner_instance_id text DEFAULT ''::text NOT NULL,
    connection_owner_acquired_at timestamp with time zone,
    connection_owner_heartbeat_at timestamp with time zone,
    cleanup_status text DEFAULT ''::text NOT NULL,
    cleanup_reason text DEFAULT ''::text NOT NULL,
    cleanup_at timestamp with time zone,
    debug_workspace_host_path text DEFAULT ''::text NOT NULL,
    debug_workspace_container_path text DEFAULT '/workspace'::text NOT NULL,
    debug_template_path text DEFAULT ''::text NOT NULL,
    debug_archived_template_path text DEFAULT ''::text NOT NULL,
    debug_vscode_launch_created boolean DEFAULT false NOT NULL,
    debug_vscode_launch_preserved boolean DEFAULT false NOT NULL,
    debug_pycharm_doc_created boolean DEFAULT false NOT NULL,
    debug_pycharm_doc_preserved boolean DEFAULT false NOT NULL,
    debug_workspace_prepared_at timestamp with time zone,
    debug_workspace_last_error text DEFAULT ''::text NOT NULL,
    CONSTRAINT ck_runtime_registry_cleanup_status CHECK (((cleanup_status = ''::text) OR (cleanup_status = ANY (ARRAY['succeeded'::text, 'failed'::text, 'user_owned'::text])))),
    CONSTRAINT ck_runtime_registry_connection_owner CHECK ((((connection_owner_instance_id = ''::text) AND (connection_owner_acquired_at IS NULL) AND (connection_owner_heartbeat_at IS NULL)) OR ((connection_owner_instance_id <> ''::text) AND (connection_owner_acquired_at IS NOT NULL) AND (connection_owner_heartbeat_at IS NOT NULL)))),
    CONSTRAINT ck_runtime_registry_ended_reason CHECK (((ended_reason = ''::text) OR (ended_reason = ANY (ARRAY['user_cancelled'::text, 'runtime_exited'::text, 'heartbeat_stale'::text, 'provision_failed'::text, 'auth_failed'::text, 'control_panel_shutdown'::text])))),
    CONSTRAINT ck_runtime_registry_lifecycle CHECK ((((status = ANY (ARRAY['starting'::text, 'active'::text, 'unhealthy'::text])) AND (ended_at IS NULL) AND (ended_reason = ''::text) AND ((status <> 'active'::text) OR (started_at IS NOT NULL))) OR ((status = ANY (ARRAY['heartbeat_stale'::text, 'ended'::text, 'cancelled'::text, 'failed'::text])) AND (ended_at IS NOT NULL) AND (ended_reason <> ''::text)))),
    CONSTRAINT ck_runtime_registry_name CHECK ((name ~ '^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$'::text)),
    CONSTRAINT ck_runtime_registry_role CHECK ((role = ANY (ARRAY['executor'::text, 'debugger'::text]))),
    CONSTRAINT ck_runtime_registry_source CHECK ((source = ANY (ARRAY['hosted'::text, 'self_hosted'::text, 'bare'::text]))),
    CONSTRAINT ck_runtime_registry_status CHECK ((status = ANY (ARRAY['starting'::text, 'active'::text, 'unhealthy'::text, 'heartbeat_stale'::text, 'ended'::text, 'cancelled'::text, 'failed'::text])))
);


--
-- Name: session_market_data_subscriptions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS session_market_data_subscriptions (
    subscription_id bigint NOT NULL,
    user_id bigint NOT NULL,
    session_id text NOT NULL,
    runtime_id text NOT NULL,
    exchange text DEFAULT 'binance'::text NOT NULL,
    market text NOT NULL,
    kind text DEFAULT 'kline'::text NOT NULL,
    symbol text NOT NULL,
    "interval" text DEFAULT ''::text NOT NULL,
    environment integer NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    released_at timestamp with time zone,
    CONSTRAINT session_market_data_subscriptions_kind_check CHECK ((kind = ANY (ARRAY['kline'::text, 'funding_rate'::text, 'open_interest'::text, 'orderbook'::text]))),
    CONSTRAINT session_market_data_subscriptions_market_check CHECK ((market = ANY (ARRAY['spot'::text, 'futures'::text]))),
    CONSTRAINT session_market_data_subscriptions_status_check CHECK ((status = ANY (ARRAY['active'::text, 'released'::text, 'failed'::text])))
);


--
-- Name: session_market_data_subscriptions_subscription_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS session_market_data_subscriptions_subscription_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: session_market_data_subscriptions_subscription_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE session_market_data_subscriptions_subscription_id_seq OWNED BY session_market_data_subscriptions.subscription_id;


--
-- Name: stream_delivery_failures; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS stream_delivery_failures (
    failure_id bigint NOT NULL,
    subscription_id bigint DEFAULT 0 NOT NULL,
    owner_instance_id text DEFAULT ''::text NOT NULL,
    topic text DEFAULT ''::text NOT NULL,
    stream_key text DEFAULT ''::text NOT NULL,
    failure_code text DEFAULT ''::text NOT NULL,
    reason text NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    attempt_count integer DEFAULT 1 NOT NULL,
    CONSTRAINT stream_delivery_failures_attempt_count_check CHECK ((attempt_count > 0)),
    CONSTRAINT stream_delivery_failures_reason_check CHECK ((reason <> ''::text))
);


--
-- Name: stream_delivery_failures_failure_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE IF NOT EXISTS stream_delivery_failures_failure_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stream_delivery_failures_failure_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE stream_delivery_failures_failure_id_seq OWNED BY stream_delivery_failures.failure_id;


--
-- Name: stream_delivery_leases; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS stream_delivery_leases (
    lease_id text NOT NULL,
    subscription_id bigint NOT NULL,
    owner_instance_id text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    acquired_at timestamp with time zone DEFAULT now() NOT NULL,
    last_heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    released_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_delivery_at timestamp with time zone,
    last_topic text DEFAULT ''::text NOT NULL,
    last_partition integer DEFAULT 0 NOT NULL,
    last_offset bigint DEFAULT 0 NOT NULL,
    CONSTRAINT stream_delivery_leases_owner_instance_id_check CHECK ((owner_instance_id <> ''::text)),
    CONSTRAINT stream_delivery_leases_status_check CHECK ((status = ANY (ARRAY['active'::text, 'released'::text, 'expired'::text, 'stolen'::text])))
);


--
-- Name: market_data_coverage_segments segment_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY market_data_coverage_segments ALTER COLUMN segment_id SET DEFAULT nextval('market_data_coverage_segments_segment_id_seq'::regclass);


--
-- Name: market_data_leases lease_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY market_data_leases ALTER COLUMN lease_id SET DEFAULT nextval('market_data_leases_lease_id_seq'::regclass);


--
-- Name: market_data_requests request_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY market_data_requests ALTER COLUMN request_id SET DEFAULT nextval('market_data_requests_request_id_seq'::regclass);


--
-- Name: market_data_streams stream_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY market_data_streams ALTER COLUMN stream_id SET DEFAULT nextval('market_data_streams_stream_id_seq'::regclass);


--
-- Name: runtime_admission_failures admission_failure_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY runtime_admission_failures ALTER COLUMN admission_failure_id SET DEFAULT nextval('runtime_admission_failures_admission_failure_id_seq'::regclass);


--
-- Name: session_market_data_subscriptions subscription_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY session_market_data_subscriptions ALTER COLUMN subscription_id SET DEFAULT nextval('session_market_data_subscriptions_subscription_id_seq'::regclass);


--
-- Name: stream_delivery_failures failure_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY stream_delivery_failures ALTER COLUMN failure_id SET DEFAULT nextval('stream_delivery_failures_failure_id_seq'::regclass);


--
-- Name: market_data_coverage_segments market_data_coverage_segments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_coverage_segments') AND conname = 'market_data_coverage_segments_pkey'
) THEN
ALTER TABLE ONLY market_data_coverage_segments
    ADD CONSTRAINT market_data_coverage_segments_pkey PRIMARY KEY (segment_id);
END IF;
END
$baseline$;


--
-- Name: market_data_history_requests market_data_history_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_history_requests') AND conname = 'market_data_history_requests_pkey'
) THEN
ALTER TABLE ONLY market_data_history_requests
    ADD CONSTRAINT market_data_history_requests_pkey PRIMARY KEY (request_id);
END IF;
END
$baseline$;


--
-- Name: market_data_leases market_data_leases_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_leases') AND conname = 'market_data_leases_pkey'
) THEN
ALTER TABLE ONLY market_data_leases
    ADD CONSTRAINT market_data_leases_pkey PRIMARY KEY (lease_id);
END IF;
END
$baseline$;


--
-- Name: market_data_leases market_data_leases_session_id_stream_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_leases') AND conname = 'market_data_leases_session_id_stream_id_key'
) THEN
ALTER TABLE ONLY market_data_leases
    ADD CONSTRAINT market_data_leases_session_id_stream_id_key UNIQUE (session_id, stream_id);
END IF;
END
$baseline$;


--
-- Name: market_data_requests market_data_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_requests') AND conname = 'market_data_requests_pkey'
) THEN
ALTER TABLE ONLY market_data_requests
    ADD CONSTRAINT market_data_requests_pkey PRIMARY KEY (request_id);
END IF;
END
$baseline$;


--
-- Name: market_data_streams market_data_streams_exchange_market_kind_symbol_interval_key; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_streams') AND conname = 'market_data_streams_exchange_market_kind_symbol_interval_key'
) THEN
ALTER TABLE ONLY market_data_streams
    ADD CONSTRAINT market_data_streams_exchange_market_kind_symbol_interval_key UNIQUE (exchange, market, kind, symbol, "interval");
END IF;
END
$baseline$;


--
-- Name: market_data_streams market_data_streams_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_streams') AND conname = 'market_data_streams_pkey'
) THEN
ALTER TABLE ONLY market_data_streams
    ADD CONSTRAINT market_data_streams_pkey PRIMARY KEY (stream_id);
END IF;
END
$baseline$;


--
-- Name: market_data_writer_leases market_data_writer_leases_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_writer_leases') AND conname = 'market_data_writer_leases_pkey'
) THEN
ALTER TABLE ONLY market_data_writer_leases
    ADD CONSTRAINT market_data_writer_leases_pkey PRIMARY KEY (lease_id);
END IF;
END
$baseline$;


--
-- Name: runtime_admission_failures runtime_admission_failures_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('runtime_admission_failures') AND conname = 'runtime_admission_failures_pkey'
) THEN
ALTER TABLE ONLY runtime_admission_failures
    ADD CONSTRAINT runtime_admission_failures_pkey PRIMARY KEY (admission_failure_id);
END IF;
END
$baseline$;


--
-- Name: runtime_channel_leases runtime_channel_leases_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('runtime_channel_leases') AND conname = 'runtime_channel_leases_pkey'
) THEN
ALTER TABLE ONLY runtime_channel_leases
    ADD CONSTRAINT runtime_channel_leases_pkey PRIMARY KEY (runtime_id);
END IF;
END
$baseline$;


--
-- Name: runtime_commands runtime_commands_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('runtime_commands') AND conname = 'runtime_commands_pkey'
) THEN
ALTER TABLE ONLY runtime_commands
    ADD CONSTRAINT runtime_commands_pkey PRIMARY KEY (command_id);
END IF;
END
$baseline$;


--
-- Name: runtime_credentials runtime_credentials_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('runtime_credentials') AND conname = 'runtime_credentials_pkey'
) THEN
ALTER TABLE ONLY runtime_credentials
    ADD CONSTRAINT runtime_credentials_pkey PRIMARY KEY (key_id);
END IF;
END
$baseline$;


--
-- Name: runtime_debug_datasets runtime_debug_datasets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('runtime_debug_datasets') AND conname = 'runtime_debug_datasets_pkey'
) THEN
ALTER TABLE ONLY runtime_debug_datasets
    ADD CONSTRAINT runtime_debug_datasets_pkey PRIMARY KEY (dataset_id);
END IF;
END
$baseline$;


--
-- Name: runtime_registry runtime_registry_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('runtime_registry') AND conname = 'runtime_registry_pkey'
) THEN
ALTER TABLE ONLY runtime_registry
    ADD CONSTRAINT runtime_registry_pkey PRIMARY KEY (runtime_id);
END IF;
END
$baseline$;


--
-- Name: session_market_data_subscriptions session_market_data_subscriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('session_market_data_subscriptions') AND conname = 'session_market_data_subscriptions_pkey'
) THEN
ALTER TABLE ONLY session_market_data_subscriptions
    ADD CONSTRAINT session_market_data_subscriptions_pkey PRIMARY KEY (subscription_id);
END IF;
END
$baseline$;


--
-- Name: stream_delivery_failures stream_delivery_failures_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('stream_delivery_failures') AND conname = 'stream_delivery_failures_pkey'
) THEN
ALTER TABLE ONLY stream_delivery_failures
    ADD CONSTRAINT stream_delivery_failures_pkey PRIMARY KEY (failure_id);
END IF;
END
$baseline$;


--
-- Name: stream_delivery_leases stream_delivery_leases_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('stream_delivery_leases') AND conname = 'stream_delivery_leases_pkey'
) THEN
ALTER TABLE ONLY stream_delivery_leases
    ADD CONSTRAINT stream_delivery_leases_pkey PRIMARY KEY (lease_id);
END IF;
END
$baseline$;


--
-- Name: idx_market_data_coverage_segments_end; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_coverage_segments_end ON market_data_coverage_segments USING btree (exchange, market, kind, symbol, "interval", year, segment_end_at);


--
-- Name: idx_market_data_coverage_segments_start; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_coverage_segments_start ON market_data_coverage_segments USING btree (exchange, market, kind, symbol, "interval", year, segment_start_at);


--
-- Name: idx_market_data_history_requests_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_history_requests_user ON market_data_history_requests USING btree (user_id, status);


--
-- Name: idx_market_data_leases_expires_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_leases_expires_at ON market_data_leases USING btree (expires_at) WHERE (released_at IS NULL);


--
-- Name: idx_market_data_leases_stream_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_leases_stream_active ON market_data_leases USING btree (stream_id, expires_at) WHERE (released_at IS NULL);


--
-- Name: idx_market_data_requests_stream; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_requests_stream ON market_data_requests USING btree (stream_id) WHERE (status <> 'cancelled'::text);


--
-- Name: idx_market_data_requests_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_requests_user ON market_data_requests USING btree (user_id, status);


--
-- Name: idx_market_data_streams_actual_state; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_streams_actual_state ON market_data_streams USING btree (actual_state);


--
-- Name: idx_market_data_writer_leases_collector; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_writer_leases_collector ON market_data_writer_leases USING btree (scraper_instance_id, collector_id, status, updated_at DESC);


--
-- Name: idx_market_data_writer_leases_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_writer_leases_expiry ON market_data_writer_leases USING btree (expires_at) WHERE (status = 'active'::text);


--
-- Name: idx_market_data_writer_leases_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_market_data_writer_leases_owner ON market_data_writer_leases USING btree (owner_instance_id, status, expires_at);


--
-- Name: idx_runtime_admission_failures_credential; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_admission_failures_credential ON runtime_admission_failures USING btree (credential_key_id, last_seen_at DESC) WHERE (credential_key_id <> ''::text);


--
-- Name: idx_runtime_admission_failures_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_admission_failures_user ON runtime_admission_failures USING btree (user_id, last_seen_at DESC);


--
-- Name: idx_runtime_channel_leases_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_channel_leases_expiry ON runtime_channel_leases USING btree (expires_at) WHERE (revoked_at IS NULL);


--
-- Name: idx_runtime_channel_leases_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_channel_leases_user ON runtime_channel_leases USING btree (user_id, expires_at DESC);


--
-- Name: idx_runtime_commands_deadline; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_commands_deadline ON runtime_commands USING btree (deadline_at) WHERE (status = ANY (ARRAY['queued'::text, 'sent'::text, 'acked'::text, 'running'::text]));


--
-- Name: idx_runtime_commands_runtime_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_commands_runtime_status ON runtime_commands USING btree (runtime_id, status, created_at);


--
-- Name: idx_runtime_commands_session; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_commands_session ON runtime_commands USING btree (session_id, created_at DESC) WHERE (session_id <> ''::text);


--
-- Name: idx_runtime_credentials_client_cert_fingerprint; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_credentials_client_cert_fingerprint ON runtime_credentials USING btree (client_cert_fingerprint) WHERE (client_cert_fingerprint <> ''::text);


--
-- Name: idx_runtime_credentials_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_credentials_expiry ON runtime_credentials USING btree (expires_at) WHERE ((expires_at IS NOT NULL) AND (status = ANY (ARRAY['active'::text, 'downloaded'::text])));


--
-- Name: idx_runtime_credentials_revoked; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_credentials_revoked ON runtime_credentials USING btree (revoked_at) WHERE (revoked_at IS NOT NULL);


--
-- Name: idx_runtime_credentials_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_credentials_user ON runtime_credentials USING btree (user_id, status) WHERE (status = 'active'::text);


--
-- Name: idx_runtime_credentials_user_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_credentials_user_status ON runtime_credentials USING btree (user_id, status, created_at DESC);


--
-- Name: idx_runtime_debug_datasets_active_runtime; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_debug_datasets_active_runtime ON runtime_debug_datasets USING btree (runtime_id) WHERE (state = 'active'::text);


--
-- Name: idx_runtime_debug_datasets_user_runtime_loaded; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_debug_datasets_user_runtime_loaded ON runtime_debug_datasets USING btree (user_id, runtime_id, loaded_at DESC);


--
-- Name: idx_runtime_registry_connection_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_registry_connection_owner ON runtime_registry USING btree (connection_owner_instance_id) WHERE ((connection_owner_instance_id <> ''::text) AND (status <> ALL (ARRAY['heartbeat_stale'::text, 'ended'::text, 'cancelled'::text, 'failed'::text])));


--
-- Name: idx_runtime_registry_credential_key_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_registry_credential_key_id ON runtime_registry USING btree (credential_key_id) WHERE ((credential_key_id IS NOT NULL) AND (status <> ALL (ARRAY['heartbeat_stale'::text, 'ended'::text, 'cancelled'::text, 'failed'::text])));


--
-- Name: idx_runtime_registry_status_heartbeat; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_registry_status_heartbeat ON runtime_registry USING btree (status, heartbeat_at);


--
-- Name: idx_runtime_registry_user_runtime; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_registry_user_runtime ON runtime_registry USING btree (user_id, runtime_id) WHERE (user_id IS NOT NULL);


--
-- Name: idx_session_market_data_subscriptions_runtime; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_session_market_data_subscriptions_runtime ON session_market_data_subscriptions USING btree (runtime_id, status, created_at DESC);


--
-- Name: idx_session_market_data_subscriptions_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_session_market_data_subscriptions_user ON session_market_data_subscriptions USING btree (user_id, status, created_at DESC);


--
-- Name: idx_stream_delivery_failures_subscription; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_stream_delivery_failures_subscription ON stream_delivery_failures USING btree (subscription_id, last_seen_at DESC);


--
-- Name: idx_stream_delivery_failures_topic; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_stream_delivery_failures_topic ON stream_delivery_failures USING btree (topic, last_seen_at DESC) WHERE (topic <> ''::text);


--
-- Name: idx_stream_delivery_leases_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_stream_delivery_leases_expiry ON stream_delivery_leases USING btree (expires_at) WHERE (status = 'active'::text);


--
-- Name: idx_stream_delivery_leases_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_stream_delivery_leases_owner ON stream_delivery_leases USING btree (owner_instance_id, status, expires_at);


--
-- Name: uidx_market_data_history_requests_active_per_user_window; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uidx_market_data_history_requests_active_per_user_window ON market_data_history_requests USING btree (user_id, exchange, market, kind, symbol, "interval", requested_start_at, requested_end_at) WHERE (status <> 'cancelled'::text);


--
-- Name: uidx_market_data_requests_active_per_user_stream; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uidx_market_data_requests_active_per_user_stream ON market_data_requests USING btree (user_id, exchange, market, kind, symbol, "interval") WHERE (status <> 'cancelled'::text);


--
-- Name: uq_market_data_coverage_segments_exact; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_market_data_coverage_segments_exact ON market_data_coverage_segments USING btree (exchange, market, kind, symbol, "interval", year, segment_start_at, segment_end_at);


--
-- Name: uq_market_data_writer_leases_active_domain; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_market_data_writer_leases_active_domain ON market_data_writer_leases USING btree (exchange, market, kind, symbol, "interval", year) WHERE (status = 'active'::text);


--
-- Name: uq_runtime_admission_failures_rollup; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_admission_failures_rollup ON runtime_admission_failures USING btree (user_id, credential_key_id, requested_runtime_id, requested_name, failure_code, consumed_runtime_id);


--
-- Name: uq_runtime_channel_leases_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_channel_leases_hash ON runtime_channel_leases USING btree (lease_hash) WHERE (revoked_at IS NULL);


--
-- Name: uq_runtime_commands_runtime_idempotency; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_commands_runtime_idempotency ON runtime_commands USING btree (runtime_id, idempotency_key) WHERE (idempotency_key <> ''::text);


--
-- Name: uq_runtime_credentials_consumed_runtime; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_credentials_consumed_runtime ON runtime_credentials USING btree (consumed_runtime_id) WHERE (consumed_runtime_id <> ''::text);


--
-- Name: uq_runtime_registry_active_credential_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_active_credential_key ON runtime_registry USING btree (credential_key_id) WHERE ((credential_key_id IS NOT NULL) AND (status <> ALL (ARRAY['heartbeat_stale'::text, 'ended'::text, 'cancelled'::text, 'failed'::text])));


--
-- Name: uq_runtime_registry_one_active_debugger_per_user; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_one_active_debugger_per_user ON runtime_registry USING btree (user_id) WHERE ((role = 'debugger'::text) AND (status <> ALL (ARRAY['ended'::text, 'cancelled'::text, 'failed'::text, 'heartbeat_stale'::text])));


--
-- Name: uq_runtime_registry_user_name; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_registry_user_name ON runtime_registry USING btree (user_id, name) WHERE (user_id IS NOT NULL);


--
-- Name: uq_session_market_data_subscriptions_active_environment; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_session_market_data_subscriptions_active_environment ON session_market_data_subscriptions USING btree (session_id, exchange, market, kind, symbol, "interval", environment) WHERE (status = 'active'::text);


--
-- Name: uq_stream_delivery_failures_rollup; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_stream_delivery_failures_rollup ON stream_delivery_failures USING btree (subscription_id, owner_instance_id, topic, stream_key, failure_code);


--
-- Name: uq_stream_delivery_leases_active_subscription; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX IF NOT EXISTS uq_stream_delivery_leases_active_subscription ON stream_delivery_leases USING btree (subscription_id) WHERE (status = 'active'::text);


--
-- Name: market_data_leases market_data_leases_stream_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_leases') AND conname = 'market_data_leases_stream_id_fkey'
) THEN
ALTER TABLE ONLY market_data_leases
    ADD CONSTRAINT market_data_leases_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES market_data_streams(stream_id) ON DELETE CASCADE;
END IF;
END
$baseline$;


--
-- Name: market_data_requests market_data_requests_stream_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('market_data_requests') AND conname = 'market_data_requests_stream_id_fkey'
) THEN
ALTER TABLE ONLY market_data_requests
    ADD CONSTRAINT market_data_requests_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES market_data_streams(stream_id) ON DELETE CASCADE;
END IF;
END
$baseline$;


--
-- Name: runtime_debug_datasets runtime_debug_datasets_runtime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('runtime_debug_datasets') AND conname = 'runtime_debug_datasets_runtime_id_fkey'
) THEN
ALTER TABLE ONLY runtime_debug_datasets
    ADD CONSTRAINT runtime_debug_datasets_runtime_id_fkey FOREIGN KEY (runtime_id) REFERENCES runtime_registry(runtime_id) ON DELETE CASCADE;
END IF;
END
$baseline$;


--
-- Name: stream_delivery_leases stream_delivery_leases_subscription_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $baseline$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = to_regclass('stream_delivery_leases') AND conname = 'stream_delivery_leases_subscription_id_fkey'
) THEN
ALTER TABLE ONLY stream_delivery_leases
    ADD CONSTRAINT stream_delivery_leases_subscription_id_fkey FOREIGN KEY (subscription_id) REFERENCES session_market_data_subscriptions(subscription_id) ON DELETE CASCADE;
END IF;
END
$baseline$;


--
-- Name: runtime_session_cleanup_outbox; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS runtime_session_cleanup_outbox (
    runtime_id text PRIMARY KEY
        REFERENCES runtime_registry(runtime_id) ON DELETE CASCADE,
    error_message text NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    last_error text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_runtime_session_cleanup_outbox_runtime
        CHECK (btrim(runtime_id) <> ''),
    CONSTRAINT chk_runtime_session_cleanup_outbox_error
        CHECK (btrim(error_message) <> ''),
    CONSTRAINT chk_runtime_session_cleanup_outbox_attempts
        CHECK (attempt_count >= 0)
);


--
-- Name: idx_runtime_session_cleanup_outbox_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_runtime_session_cleanup_outbox_due
    ON runtime_session_cleanup_outbox (next_attempt_at, created_at, runtime_id);


--
-- PostgreSQL database dump complete
--
