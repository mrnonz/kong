local pdk_tracer = require "kong.pdk.tracing".new()
local utils = require "kong.tools.utils"
local tablepool = require "tablepool"
local tablex = require "pl.tablex"
local hooks = require "kong.hooks"
local base = require "resty.core.base"

local ngx = ngx
local var = ngx.var
local pack = utils.pack
local unpack = utils.unpack
local new_tab = base.new_tab
local time_ns = utils.time_ns
local tablepool_release = tablepool.release
local get_method = ngx.req.get_method

local _M = {}
local tracer = pdk_tracer
local NOOP = function() end
local available_types = {}

local POOL_SPAN_STORAGE = "KONG_SPAN_STORAGE"

-- db query
do
  -- name mapping
  local db_system_map = {
    postgres = "postgresql",
    cassandra = "cassandra",
  }

  function _M.db_query(connector)
    local f = connector.query

    local function wrap(self, sql, ...)
      local span = tracer.start_span("query", {
        attributes = {
          ["db.system"] = db_system_map[kong.db.strategy],
          ["db.statement"] = sql,
        }
      })
      local ret = pack(f(self, sql, ...))
      span:finish()
      return unpack(ret)
    end

    connector.query = wrap
  end
end

-- router
function _M.router()
  hooks.register_hook("runloop:access:router:pre", function(ctx)
    return tracer.start_span("router")
  end)

  hooks.register_hook("runloop:access:router:post", function(span, match_t)
    if not span then
      return
    end

    span:finish()
  end)
end

-- request (root span)
-- we won't set the propagation headers there to avoid conflict with other tracing plugins
function _M.request(ctx)
  local req = kong.req
  local client = kong.client

  local method = get_method()
  local path = req.get_path()
  local span_name = method .. " " .. path
  local req_uri = ctx.request_uri or var.request_uri

  local start_time = ngx.ctx.KONG_PROCESSING_START
      and ngx.ctx.KONG_PROCESSING_START * 100000
      or time_ns()

  local active_span = tracer.start_span(span_name, {
    start_time_ns = start_time,
    attributes = {
      ["http.method"] = method,
      ["http.url"] = req_uri,
      ["http.host"] = var.http_host,
      ["http.scheme"] = ctx.scheme or var.scheme,
      ["http.flavor"] = ngx.req.http_version(),
      ["net.peer.ip"] = client.get_ip(),
    },
  })
  tracer.set_active_span(active_span)
end

-- balancer
function _M.balancer(ctx)
  local balancer_data = ctx.balancer_data
  if not balancer_data then
    return
  end

  local span
  local balancer_tries = balancer_data.tries
  local try_count = balancer_data.try_count
  for i = 1, try_count do
    local try = balancer_tries[i]
    span = tracer.start_span("balancer try #" .. i, {
      kind = 3, -- client
      start_time_ns = try.balancer_start * 100000000,
      attributes = {
        ["kong.balancer.state"] = try.state,
        ["http.status_code"] = try.code,
        ["net.peer.ip"] = try.ip,
        ["net.peer.port"] = try.port,
      }
    })

    if i < try_count then
      span:set_status(2)
    end

    if try.balancer_latency ~= nil then
      span:finish((try.balancer_start + try.balancer_latency) * 100000000)
    else
      span:finish()
    end
  end
end

local function register_plugin_hook(phase)
  return function()
    hooks.register_hook("plugin:" .. phase .. ":pre", function(plugin)
      return tracer.start_span(phase .. " phase: " .. plugin.name)
    end)

    hooks.register_hook("plugin:" .. phase .. ":post", function(span)
      if not span then
        return
      end

      span:finish()
    end)
  end
end

_M.plugin_rewrite = register_plugin_hook("rewrite")
_M.plugin_access = register_plugin_hook("access")
_M.plugin_header_filter = register_plugin_hook("header_filter")
-- TODO: support body_filter

function _M.dns_query(host, port)
  return tracer.start_span("DNS: " .. host)
end

function _M.http_client()
  -- patch lua-resty-http
  local http = require "resty.http"
  local request_uri = http.request_uri
  local function wrap(self, uri, params)
    local method = params and params.method or "GET"
    local attributes = new_tab(0, 5)
    attributes["http.url"] = uri
    attributes["http.method"] = method
    attributes["http.flavor"] = params and params.version or "1.1"
    attributes["http.user_agent"] = params and params.headers and params.headers["User-Agent"]
        or http._USER_AGENT

    local span = tracer.start_span("HTTP " .. method .. " " .. uri, {
      attributes = attributes,
    })
    local res, err = request_uri(self, uri, params)
    if res then
      attributes["http.status_code"] = res.status -- number
    else
      span:record_error(err)
    end
    span:finish()

    return res, err
  end

  http.request_uri = wrap
end

-- regsiter available_types
for k, _ in pairs(_M) do
  available_types[k] = true
end
_M.available_types = available_types

function _M.patch_dns_query(func)
  return function(host, port)
    local span = _M.dns_query(host, port)
    local ip_addr, res_port, try_list = func(host, port)
    if span then
      span:set_attribute("dns.record.ip", ip_addr)
      span:finish()
    end

    return ip_addr, res_port, try_list
  end
end

function _M.runloop_log_before(ctx)
  -- add balancer
  _M.balancer(ctx)

  local active_span = tracer.active_span()
  -- check root span type to avoid encounter error
  if active_span and type(active_span.finish) == "function" then
    active_span:finish()
  end
end

function _M.runloop_log_after(ctx)
  -- Clears the span table and put back the table pool,
  -- this avoids reallocation.
  -- The span table MUST NOT be used after released.
  if type(ctx.KONG_SPANS) == "table" then
    for _, span in ipairs(ctx.KONG_SPANS) do
      if type(span) == "table" and type(span.release) == "function" then
        span:release()
      end
    end

    tablepool_release(POOL_SPAN_STORAGE, ctx.KONG_SPANS)
  end
end

function _M.init(config)
  local trace_types = config.opentelemetry_tracing
  local sampling_rate = config.opentelemetry_tracing_sampling_rate
  assert(type(trace_types) == "table" and #trace_types > 0)
  assert(sampling_rate >= 0 and sampling_rate <= 1)

  local enabled = trace_types[1] ~= "off"

  -- noop instrumentations
  -- TODO: support stream module
  if not enabled or ngx.config.subsystem == "stream" then
    for k, _ in pairs(available_types) do
      _M[k] = NOOP
    end
  end

  if trace_types[1] ~= "all" then
    for k, _ in pairs(available_types) do
      if not tablex.find(trace_types, k) then
        _M[k] = NOOP
      end
    end
  end

  if enabled then
    -- global tracer
    tracer = pdk_tracer.new("instrument", {
      sampling_rate = sampling_rate,
    })
    tracer.set_global_tracer(tracer)

    -- register hooks
    _M.router()
    _M.plugin_rewrite()
    _M.plugin_access()
    _M.plugin_header_filter()
  end
end

return _M
