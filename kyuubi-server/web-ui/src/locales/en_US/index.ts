/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export default {
  test: 'test',
  user: 'User',
  client_ip: 'Client IP',
  server_ip: 'Server IP',
  kyuubi_instance: 'Kyuubi Instance',
  session_type: 'Type',
  status: 'Status',
  driver_pod: 'Driver Pod',
  search: 'Search',
  reconnecting: 'Reconnecting',
  driver_log: 'Driver Log',
  driver_events: 'Driver Events',
  no_events: 'No events',
  driver_state: 'Driver State',
  refresh: 'Refresh',
  owner_down_hint:
    'Owning instance is unreachable — the batch is still running and will reattach when the instance returns or a peer takes it over.',
  session_id: 'Session ID',
  operation_id: 'Operation ID',
  batch_id: 'Batch ID',
  batch_name: 'Batch Name',
  batch_type: 'Batch Type',
  create_time: 'Create Time',
  end_time: 'End Time',
  start_time: 'State Time',
  complete_time: 'Completed Time',
  state: 'State',
  app_id: 'Application ID',
  app_url: 'Application URL',
  app_state: 'Application State',
  app_diagnostic: 'Application Diagnostic',
  duration: 'Duration',
  statement: 'Statement',
  engine_address: 'Engine Address',
  engine_id: 'Engine ID',
  engine_type: 'Engine Type',
  share_level: 'Share Level',
  version: 'Version',
  engine_ui: 'Engine UI',
  failure_reason: 'Failure Reason',
  session_properties: 'Session Properties',
  no_data: 'No data',
  no_log: 'No log',
  summary: {
    total: 'Total',
    active: 'Active',
    idle: 'Idle',
    error: 'Error',
    reconnecting: 'Reconnecting'
  },
  overview: {
    eyebrow: 'Nexus One → SparkEngine',
    title: 'Cluster overview',
    subtitle:
      'Live view of the Kyuubi servers, engines and sessions backing this workspace.',
    activity: 'Activity',
    cluster: 'Cluster',
    servers: 'Servers',
    engines: 'Engines',
    sessions: 'Sessions',
    running: 'Running',
    failed: 'Failed',
    version: 'Version',
    engine_types: 'Engine types',
    active_users: 'Active users',
    instances: 'Instances',
    unavailable: 'Cluster details are unavailable. Sign in to view them.'
  },
  follow: 'Follow',
  log_truncated: 'Only the latest {count} lines are shown',
  previous: 'Previous',
  next: 'Next',
  run_sql_tips: 'Run a SQL to get result',
  result: 'Result',
  log: 'Log',
  operation: {
    text: 'Operation',
    delete_confirm: 'Delete Confirm',
    close_confirm: 'Close Confirm',
    cancel_confirm: 'Cancel Confirm',
    close: 'Close',
    cancel: 'Cancel',
    delete: 'Delete',
    run: 'Run'
  },
  login: {
    invalid_credentials: 'Invalid username or password.',
    server_error: 'Server error, please try again later.',
    failed: 'Login failed, please try again.',
    sso_hint: 'This workspace uses single sign-on.',
    sso_button: 'Continue with SSO',
    signing_in: 'Signing you in…'
  },
  data_agent: {
    title: 'Data Agent',
    welcome_desc:
      'Ask questions about your data in natural language. The agent will explore schemas, write SQL queries, and analyze results.',
    connection: 'Connection',
    jdbc_url: 'JDBC URL',
    server_default: 'Server default',
    jdbc_placeholder: 'Leave empty to use server default',
    try_asking: 'Try asking',
    quick_tables: 'What tables are available?',
    quick_schema: 'Describe the columns of a table',
    quick_records: 'Show me a few sample rows',
    auto_approve: 'Auto Approve',
    normal: 'Normal',
    strict: 'Strict',
    approval_tooltip:
      'Controls whether tool calls require your approval before execution',
    session_label: 'Session',
    session_tooltip: 'Session ID: {id}',
    datasource_label: 'Data:',
    datasource_tooltip_default:
      'Using server default data source (no JDBC URL specified)',
    model_placeholder: 'Model (optional)',
    model_tooltip:
      'Override the LLM model for this conversation. Leave empty to use the engine default.',
    stop: 'Stop',
    change_jdbc:
      'Start a new conversation from the right panel to change JDBC URL',
    starting_engine: 'Starting Data Agent engine...',
    waiting_response: 'Waiting for response...',
    input_placeholder: 'Ask a question about your data...',
    input_hint_send: 'send',
    input_hint_newline: 'new line',
    session_expired:
      'Session has expired. Start a new conversation from the panel on the right.',
    session_start_failed: 'Failed to start session: {message}',
    session_close_failed: 'Failed to close previous session, starting fresh.',
    malformed_response: 'Received malformed response from server',
    stream_error: 'Stream error',
    stream_incomplete: 'Response stream ended unexpectedly',
    engine_unresponsive:
      'The data agent engine is not responding. The session may be stale.',
    reset_session: 'New session',
    unknown_error: 'Unknown error',
    approval_failed: 'Approval failed: {message}',
    approval_not_found: 'Approval request is no longer pending',
    approval_required: 'Approval Required',
    arguments: 'Arguments',
    result: 'Result',
    approve: 'Approve',
    deny: 'Deny',
    approved: 'Approved',
    denied: 'Denied',
    running: 'Running...',
    done: 'Done',
    error: 'Error',
    generating: 'Generating...',
    copied: 'Copied',
    copy_failed: 'Copy failed',
    copy: 'Copy',
    history: 'Recent',
    conversations: 'Conversations',
    new_conversation: 'New conversation',
    untitled_session: 'New chat',
    close_conversation: 'Close conversation',
    close_conversation_confirm:
      'Close conversation "{title}"? This cannot be undone.',
    close_conversation_confirm_ok: 'Close',
    close_conversation_confirm_cancel: 'Cancel',
    rename_conversation: 'Rename',
    expand_rail: 'Expand panel',
    collapse_rail: 'Collapse panel',
    token_usage_tooltip: 'Context: {lastPrompt} • Output: {lastCompletion}',
    tokens_in: 'in',
    tokens_out: 'out',
    thinking: 'Thinking…',
    thoughts: 'Thoughts'
  },
  spark_connect: {
    eyebrow: 'Nexus One → SparkEngine',
    title: 'Spark Connect',
    subtitle:
      'Point a Spark client at this gateway using the credential you already have. You get one session, with an engine of your own behind it.',
    new_session: 'New session',
    no_session: 'You have no Spark Connect session yet.',
    conf_hint:
      'Optional Spark configuration for this session. Keys that only Kyuubi may set are ignored by the server.',
    conf_key_placeholder: 'spark.sql.shuffle.partitions',
    conf_value_placeholder: 'Value',
    add_conf: 'Add configuration',
    remove_conf: 'Remove configuration',
    create_session: 'Create session',
    create_note:
      'The engine takes a minute or two to start. The client retries until it is ready.',
    your_session: 'Your session',
    pending_note: 'The engine is still starting.',
    connect_url: 'Connect URL',
    connect_url_note:
      'This is the address the server advertises. If it is not reachable from where you run Spark, ask your administrator to set kyuubi.frontend.advertised.host.',
    credential_note:
      'Authenticate with the same credential you use for the Kyuubi REST API. Kyuubi issues no token of its own for Spark Connect.',
    copy: 'Copy',
    snippet: 'PySpark',
    copy_snippet: 'Copy snippet',
    close_confirm: 'Close this session and stop its engine?',
    copied: 'Copied',
    copy_failed: 'Copy failed — select the text and copy it manually',
    create_succeeded: 'Session created',
    create_failed: 'Failed to create session: {message}',
    close_succeeded: 'Session closed',
    close_failed: 'Failed to close session: {message}',
    list_failed: 'Failed to load your session: {message}',
    unknown_error: 'Unknown error'
  },
  message: {
    delete_succeeded: 'Delete {name} Succeeded',
    delete_failed: 'Delete {name} Failed',
    close_succeeded: 'Close {name} Succeeded',
    close_failed: 'Close {name} Failed',
    cancel_succeeded: 'Cancel {name} Succeeded',
    cancel_failed: 'Cancel {name} Failed',
    run_sql_failed: 'Run SQL Failed',
    get_batches_failed: 'Get Batches Failed',
    get_sql_log_failed: 'Get SQL Log Failed',
    get_batch_log_failed: 'Get Batch Log Failed',
    get_sql_result_failed: 'Get SQL Result Failed',
    get_sql_metadata_failed: 'Get SQL Metadata Failed'
  }
}
