import { useEffect, useRef, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { apiGet, buildWsUrl } from "./lib/api";
import {
  fmtCompact,
  fmtDateTime,
  fmtInt,
  formatDisplayNumber,
  formatPercent,
  formatPriceStep,
  shortKey,
  valueOrDash,
  windowLabel,
} from "./lib/format";

const WINDOW_OPTIONS = [
  { value: "60", label: "60m" },
  { value: "360", label: "6h" },
  { value: "1440", label: "24h" },
  { value: "10080", label: "7d" },
];

const LIMIT_OPTIONS = [
  { value: "10", label: "10" },
  { value: "20", label: "20" },
  { value: "50", label: "50" },
];

const EVENT_COLORS = ["#61f7c6", "#8fe388", "#4db6ff", "#ffd166", "#ff7a90", "#b794f4", "#f59f00", "#94d82d"];
const SERIES_COLORS = { swaps: "#61f7c6" };

export function App() {
  const [minutes, setMinutes] = useState("1440");
  const [limit, setLimit] = useState("10");
  const [backendHealthy, setBackendHealthy] = useState(false);
  const [streamState, setStreamState] = useState("connecting");
  const [lastSnapshotAt, setLastSnapshotAt] = useState(0);
  const [dashboard, setDashboard] = useState(null);
  const [selectedPool, setSelectedPool] = useState("");
  const [poolInput, setPoolInput] = useState("");
  const [poolExplorer, setPoolExplorer] = useState(null);
  const selectedPoolRef = useRef("");
  const lastAnchorRef = useRef(0);

  useEffect(() => {
    void refreshHealth();
  }, []);

  useEffect(() => {
    const socket = new WebSocket(buildWsUrl("/ws/dashboard", { minutes, limit }));
    setStreamState("connecting");

    socket.addEventListener("open", () => setStreamState("live"));
    socket.addEventListener("message", (event) => {
      try {
        const incoming = JSON.parse(event.data);
        if (incoming.type === "error") {
          return;
        }
        const nextDashboard = incoming.payload || {};
        const anchor = Number(nextDashboard.anchor_unix_ms || 0);
        if (anchor > 0 && anchor < lastAnchorRef.current) {
          return;
        }
        lastAnchorRef.current = anchor;
        setDashboard(nextDashboard);
        setLastSnapshotAt(Date.now());

        const candidatePool = nextDashboard?.top_pools?.[0]?.pool || "";
        if (candidatePool && !selectedPoolRef.current) {
          selectedPoolRef.current = candidatePool;
          setSelectedPool(candidatePool);
          setPoolInput(candidatePool);
        }
      } catch {
        setStreamState("error");
      }
    });
    socket.addEventListener("close", () => setStreamState("closed"));
    socket.addEventListener("error", () => setStreamState("error"));
    return () => socket.close();
  }, [minutes, limit]);

  useEffect(() => {
    selectedPoolRef.current = selectedPool;
    if (!selectedPool) {
      return;
    }
    void loadPoolExplorer(selectedPool, minutes);
  }, [selectedPool, minutes]);

  async function refreshHealth() {
    try {
      const health = await apiGet("/health");
      setBackendHealthy(Boolean(health.clickhouse_ok));
    } catch {
      setBackendHealthy(false);
    }
  }

  async function loadPoolExplorer(pool, activeMinutes) {
    try {
      const explorer = await apiGet(`/v1/pools/${encodeURIComponent(pool)}/explorer`, { minutes: activeMinutes });
      setPoolExplorer(explorer);
    } catch {
      // keep previous explorer state on transient failures
    }
  }

  async function refreshDashboardSnapshot() {
    try {
      const nextDashboard = await apiGet("/v1/analytics/dashboard", {
        minutes,
        limit,
        anchor_unix_ms: Date.now(),
      });
      const anchor = Number(nextDashboard.anchor_unix_ms || 0);
      if (anchor > 0) {
        lastAnchorRef.current = anchor;
      }
      setDashboard(nextDashboard);
      setLastSnapshotAt(Date.now());
    } catch {
      // keep live websocket state as source of truth on transient refresh failures
    }
  }

  const overview = dashboard?.overview || {};
  const metrics = overview.metrics || {};
  const eventMix = dashboard?.event_mix || [];
  const topPools = dashboard?.top_pools || [];
  const swapsSeries = seriesForChart(dashboard?.swaps_series || [], "swaps");
  const eventMixSeries = eventMix.map((item, index) => ({
    event: compactEventName(item.event_name),
    count: Number(item.event_count || 0),
    fill: EVENT_COLORS[index % EVENT_COLORS.length],
  }));

  const market = poolExplorer?.market || {};
  const config = market.pool_config || {};
  const binRows = market.bins || [];

  return (
    <div className="terminal-app">
      <header className="topbar topbar--compact">
        <div className="topbar__brand">
          <div className="eyebrow">solana / meteora / dlmm</div>
          <h1>DUNE PROJECT TERMINAL</h1>
          <p>real-time protocol analytics over live dlmm events</p>
        </div>

        <div className="topbar__controls">
          <label className="control">
            <span>window</span>
            <select value={minutes} onChange={(event) => setMinutes(event.target.value)}>
              {WINDOW_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className="control">
            <span>top pools</span>
            <select value={limit} onChange={(event) => setLimit(event.target.value)}>
              {LIMIT_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <button className="button" type="button" onClick={() => window.location.reload()}>reconnect</button>
        </div>
      </header>

      <section className="status-strip status-strip--minimal">
        <StatusPill label="backend" value={backendHealthy ? "online" : "degraded"} tone={backendHealthy ? "green" : "red"} />
        <StatusPill label="stream" value={streamState} tone={streamState === "live" ? "cyan" : "amber"} />
        <StatusPill label="last stream update" value={fmtDateTime(lastSnapshotAt)} tone="white" />
      </section>

      <section className="metric-grid metric-grid--four">
        <AnimatedMetricTile label="swaps" rawValue={metrics.total_swaps || 0} tone="green" formatter={fmtInt} />
        <AnimatedMetricTile label="observed users" rawValue={metrics.active_users || 0} tone="amber" formatter={fmtInt} sublabel={`within ${windowLabel(Number(minutes))}`} />
        <AnimatedMetricTile label="observed pools" rawValue={metrics.active_pools || 0} tone="violet" formatter={fmtInt} sublabel={`active in ${windowLabel(Number(minutes))}`} />
        <AnimatedMetricTile label="claim events" rawValue={metrics.claim_events || 0} tone="pink" formatter={fmtInt} />
      </section>

      <section className="layout-grid layout-grid--charts-two compact-top">
        <Panel title="SWAPS OVER TIME" accent="green">
          <ChartFrame><SeriesChart data={swapsSeries} dataKey="swaps" color={SERIES_COLORS.swaps} /></ChartFrame>
        </Panel>
        <Panel title="EVENT MIX" accent="violet">
          <EventMixList data={eventMixSeries} />
        </Panel>
      </section>

      <section className="layout-grid layout-grid--mid compact-top">
        <Panel
          title="TOP POOLS"
          accent="cyan"
          actions={<button className="panel-action" type="button" onClick={refreshDashboardSnapshot}>refresh</button>}
        >
          <DataTable
            className="data-table--top-pools"
            headers={["pool", "swaps", "active bin id", "bin step", "price step", "fee %"]}
            rows={topPools.map((pool) => ({
              key: pool.pool,
              cells: [
                topPoolAddressCell(pool.pool),
                fmtInt(pool.swap_count),
                valueOrDash(pool.active_bin_id),
                valueOrDash(pool.bin_step),
                formatPriceStep(pool.bin_step),
                formatPercent(pool.total_fee_pct),
              ],
              onClick: () => {
                selectedPoolRef.current = pool.pool;
                setSelectedPool(pool.pool);
                setPoolInput(pool.pool);
              },
              active: pool.pool === selectedPool,
            }))}
          />
        </Panel>
      </section>

      <section className="layout-grid layout-grid--explorer-single compact-top">
        <Panel title="POOL EXPLORER" accent="pink">
          <div className="explorer-quad compact-top">
            <SubPanel title="POOL SEARCH" className="explorer-quad__search">
              <div className="explorer-toolbar">
                <input className="text-input" value={poolInput} onChange={(event) => setPoolInput(event.target.value)} placeholder="paste pool pubkey" />
                <button className="button" type="button" onClick={() => {
                  const nextPool = poolInput.trim();
                  if (!nextPool) {
                    return;
                  }
                  selectedPoolRef.current = nextPool;
                  setSelectedPool(nextPool);
                }}>load pool</button>
              </div>
              <KeyValueList
                items={[
                  ["pool", selectedPool ? fullAddressCell(selectedPool) : "-"],
                  ["derived price", formatDisplayNumber(market.current_price)],
                  ["active bin id", valueOrDash(market.active_bin_id)],
                  ["bin step", valueOrDash(config.bin_step)],
                  ["price step", formatPriceStep(config.bin_step)],
                  ["token x", market.mint_x ? fullAddressCell(market.mint_x) : "-"],
                  ["token y", market.mint_y ? fullAddressCell(market.mint_y) : "-"],
                ]}
              />
            </SubPanel>

            <SubPanel title="LIQUIDITY DISTRIBUTION" className="explorer-quad__liquidity">
              <BinDistribution bins={binRows} tokenX={market.token_x_symbol} tokenY={market.token_y_symbol} error={market.error} />
            </SubPanel>

            <SubPanel title="POOL CONFIG" className="explorer-quad__config">
              <KeyValueList
                items={[
                  ["name", valueOrDash(market.name)],
                  ["token x decimals", valueOrDash(market.token_x_decimals)],
                  ["token y decimals", valueOrDash(market.token_y_decimals)],
                  ["base fee %", valueOrDash(config.base_fee_pct)],
                  ["variable fee %", valueOrDash(market.dynamic_fee_pct)],
                  ["total fee %", valueOrDash(config.max_fee_pct)],
                  ["protocol fee %", valueOrDash(config.protocol_fee_pct)],
                  ["protocol fee x", valueOrDash(market.protocol_fee_x_raw)],
                  ["protocol fee y", valueOrDash(market.protocol_fee_y_raw)],
                  ["reserve x", reserveLabel(market.reserve_x)],
                  ["reserve y", reserveLabel(market.reserve_y)],
                  ["populated bins", fmtInt(market.populated_bin_count || 0)],
                ]}
              />
            </SubPanel>
          </div>
        </Panel>
      </section>
    </div>
  );
}

function tokenLabel(symbol, mint) {
  if (symbol && mint) {
    return <span>{symbol} · {clickableAddress(mint)}</span>;
  }
  if (symbol) {
    return symbol;
  }
  if (mint) {
    return clickableAddress(mint);
  }
  return "-";
}

function topPoolAddressCell(poolAddress) {
  const full = String(poolAddress || "");
  if (!full) {
    return "-";
  }
  return (
    <div className="pool-address-cell" title={full}>
      <strong>{full}</strong>
      <button
        type="button"
        className="copy-icon-button"
        onClick={(event) => {
          event.stopPropagation();
          void navigator.clipboard.writeText(full);
        }}
        title={`copy ${full}`}
        aria-label="copy pool address"
      >
        ⧉
      </button>
    </div>
  );
}

function fullAddressCell(value) {
  const full = String(value || "");
  if (!full) {
    return "-";
  }
  return (
    <div className="pool-address-cell" title={full}>
      <strong>{full}</strong>
      <button
        type="button"
        className="copy-icon-button"
        onClick={(event) => {
          event.stopPropagation();
          void navigator.clipboard.writeText(full);
        }}
        title={`copy ${full}`}
        aria-label="copy address"
      >
        ⧉
      </button>
    </div>
  );
}

function reserveLabel(value) {
  if (!value) {
    return "-";
  }
  if (typeof value === "string" && value.length >= 32) {
    return clickableAddress(value);
  }
  return formatDisplayNumber(value);
}

function clickableAddress(value) {
  if (!value) {
    return "-";
  }
  return (
    <button
      type="button"
      className="copy-chip"
      onClick={(event) => {
        event.stopPropagation();
        void navigator.clipboard.writeText(String(value));
      }}
      title={`copy ${value}`}
    >
      {shortKey(String(value))}
    </button>
  );
}

function StatusPill({ label, value, tone }) {
  return <div className={`status-pill status-pill--${tone}`}><span>{label}</span><strong>{value}</strong></div>;
}

function MetricTile({ label, value, tone, sublabel }) {
  return (
    <div className={`metric-tile metric-tile--${tone}`}>
      <div className="metric-tile__top">
        <span>{label}</span>
        {sublabel ? <span className="metric-tile__sublabel">{sublabel}</span> : null}
      </div>
      <strong>{value}</strong>
    </div>
  );
}

function AnimatedMetricTile({ label, rawValue, tone, formatter, sublabel }) {
  const animatedValue = useAnimatedNumber(rawValue);
  return <MetricTile label={label} value={formatter(animatedValue)} tone={tone} sublabel={sublabel} />;
}

function Panel({ title, accent = "green", actions = null, children }) {
  return (
    <section className={`panel panel--${accent}`}>
      <header className="panel__header">
        <span className="panel__title">{title}</span>
        {actions ? <div className="panel__actions">{actions}</div> : null}
      </header>
      <div className="panel__body">{children}</div>
    </section>
  );
}

function SubPanel({ title, children, className = "" }) {
  return <div className={`subpanel ${className}`.trim()}><div className="subpanel__title">{title}</div>{children}</div>;
}

function DataCell({ label, value }) {
  const title = typeof value === "string" || typeof value === "number" ? String(value) : undefined;
  return <div className="data-cell"><span>{label}</span><strong title={title}>{value}</strong></div>;
}

function KeyValueList({ items }) {
  return (
    <div className="kv-list">
      {items.map(([label, value]) => {
        const title = typeof value === "string" || typeof value === "number" ? String(value) : undefined;
        return (
          <div className="kv-row" key={label}>
            <span>{label}</span>
            <strong title={title}>{value}</strong>
          </div>
        );
      })}
    </div>
  );
}

function DataTable({ headers, rows, className = "" }) {
  const tableClass = `data-table ${className}`.trim();
  return <div className="table-wrap"><table className={tableClass}><thead><tr>{headers.map((header) => <th key={header}>{header}</th>)}</tr></thead><tbody>{rows.length === 0 ? <tr><td colSpan={headers.length} className="empty-row">waiting for data</td></tr> : rows.map((row) => <tr key={row.key} className={row.active ? "is-active" : ""} onClick={row.onClick} style={row.onClick ? { cursor: "pointer" } : undefined}>{row.cells.map((cell, index) => <td key={`${row.key}-${index}`} title={typeof cell === "string" || typeof cell === "number" ? String(cell) : undefined}>{cell}</td>)}</tr>)}</tbody></table></div>;
}

function EventMixList({ data }) {
  const max = Math.max(...data.map((item) => item.count), 1);
  return <div className="mix-list">{data.length === 0 ? <div className="empty-row">waiting for data</div> : data.map((item) => <div className="mix-row" key={item.event}><span className="mix-row__label" title={item.event}>{item.event}</span><div className="mix-row__track"><div className="mix-row__fill" style={{ width: `${(item.count / max) * 100}%`, background: item.fill }} /></div><strong>{fmtInt(item.count)}</strong></div>)}</div>;
}

function ChartFrame({ children }) {
  return <div className="chart-frame">{children}</div>;
}

function BinDistribution({ bins, tokenX, tokenY, error }) {
  const [hoveredBinId, setHoveredBinId] = useState(null);
  const allRows = bins.map((bin) => {
    const amountX = Number(bin.amount_x_ui || 0);
    const amountY = Number(bin.amount_y_ui || 0);
    const price = Number(bin.price_x_per_y || 0);
    const xInQuote = Number.isFinite(price) ? amountX * price : 0;
    const yInQuote = amountY;
    const totalLiquidity = Math.max(0, xInQuote) + Math.max(0, yInQuote);
    let side = "empty";
    let dominantLiquidity = 0;
    if (xInQuote > 0 && yInQuote > 0) {
      side = "active";
      dominantLiquidity = totalLiquidity;
    } else if (xInQuote > 0) {
      side = "x";
      dominantLiquidity = xInQuote;
    } else if (yInQuote > 0) {
      side = "y";
      dominantLiquidity = yInQuote;
    }
    return {
      ...bin,
      amountX: Number.isFinite(amountX) ? amountX : 0,
      amountY: Number.isFinite(amountY) ? amountY : 0,
      price,
      xInQuote: Number.isFinite(xInQuote) ? xInQuote : 0,
      yInQuote: Number.isFinite(yInQuote) ? yInQuote : 0,
      totalLiquidity: Number.isFinite(totalLiquidity) ? totalLiquidity : 0,
      xShare: totalLiquidity > 0 ? Math.max(0, xInQuote) / totalLiquidity : 0,
      yShare: totalLiquidity > 0 ? Math.max(0, yInQuote) / totalLiquidity : 0,
      side,
      dominantLiquidity,
    };
  });
  const rows = allRows.filter((row) => row.amountX > 0 || row.amountY > 0 || row.distance_from_active === 0);
  const computedMaxLiquidity = Math.max(...rows.map((row) => row.dominantLiquidity), 0);
  const maxLiquidity = computedMaxLiquidity > 0 ? computedMaxLiquidity : 1;
  const activeRow = rows.find((row) => row.distance_from_active === 0) || null;
  const activeIndex = activeRow ? rows.findIndex((row) => row.bin_id === activeRow.bin_id) : -1;
  const hoveredRow = rows.find((row) => row.bin_id === hoveredBinId) || null;
  const hoveredIndex = hoveredRow ? rows.findIndex((row) => row.bin_id === hoveredRow.bin_id) : -1;
  const labelEvery = Math.max(1, Math.floor(rows.length / 6));
  const tickRows = rows.filter((row, index) => index % labelEvery === 0 || row.distance_from_active === 0);
  const hasHover = hoveredRow !== null;
  return (
    <div className="bin-distribution">
      {rows.length === 0 ? (
        <div className="empty-row">{error || "no populated bins returned for this pool"}</div>
      ) : (
        <>
          <div className="bin-distribution__legend">
            <span><i className="legend-dot legend-dot--x" /> {tokenX || "token x"}</span>
            <span><i className="legend-dot legend-dot--y" /> {tokenY || "token y"}</span>
            <span className="bin-distribution__note">
              current price {formatDisplayNumber(activeRow?.price)} {tokenY || "quote"}/{tokenX || "base"}
            </span>
          </div>
          <div className="bin-distribution__chart">
            {!hasHover ? (
              <div className="bin-distribution__overlay">
                <div className="bin-tooltip-card bin-tooltip-card--current">
                  <div className="bin-tooltip-card__title">Current Price</div>
                <div className="bin-tooltip-card__value">
                  {formatDisplayNumber(activeRow?.price)}
                </div>
                <div className="bin-tooltip-card__meta">
                  {activeRow?.amountX > 0 ? <span>{tokenX || "token x"} {formatDisplayNumber(activeRow.amountX)}</span> : null}
                  {activeRow?.amountY > 0 ? <span>{tokenY || "token y"} {formatDisplayNumber(activeRow.amountY)}</span> : null}
                </div>
              </div>
              </div>
            ) : null}

            <div className="bin-chart">
              <div className="bin-chart__baseline" />
              <div className="bin-chart__content">
                {rows.map((row, index) => {
                  const height = row.totalLiquidity > 0 ? Math.max(6, (row.totalLiquidity / maxLiquidity) * 100) : 0;
                  const cls = row.distance_from_active === 0 ? " bin-cluster--active" : "";
                  const faded = hasHover && hoveredRow?.bin_id !== row.bin_id ? " is-faded" : "";
                  const isHovered = hoveredRow?.bin_id === row.bin_id;
                  const hovered = isHovered ? " is-hovered" : "";
                  const barClass =
                    row.side === "x"
                      ? "bin-rect bin-rect--x"
                      : row.side === "y"
                        ? "bin-rect bin-rect--y"
                        : row.side === "active"
                          ? "bin-rect bin-rect--active-split"
                          : "bin-rect bin-rect--empty";
                  return (
                    <div
                      key={row.bin_id}
                      className={`bin-cluster${cls}${faded}${hovered}`}
                      onMouseEnter={() => setHoveredBinId(row.bin_id)}
                      onMouseLeave={() => setHoveredBinId(null)}
                      title={`bin ${row.bin_id}`}
                    >
                      {index === activeIndex ? <div className="bin-cluster__active-line" /> : null}
                      {isHovered ? (
                        <div className="bin-tooltip-card bin-tooltip-card--hover-floating">
                          <div className="bin-tooltip-card__title">Bin {row.bin_id}</div>
                          <div className="bin-tooltip-card__value">
                            {formatDisplayNumber(row.price)}
                          </div>
                          <div className="bin-tooltip-card__meta">
                            {row.amountX > 0 ? <span>{tokenX || "token x"} {formatDisplayNumber(row.amountX)}</span> : null}
                            {row.amountY > 0 ? <span>{tokenY || "token y"} {formatDisplayNumber(row.amountY)}</span> : null}
                          </div>
                        </div>
                      ) : null}
                      <div className="bin-cluster__bars">
                        <div className={barClass} style={{ height: `${height}%`, opacity: row.totalLiquidity > 0 ? 1 : 0.12 }}>
                          {row.side === "active" ? (
                            <>
                              <div className="bin-rect__seg bin-rect__seg--y" style={{ flex: row.yShare }} />
                              <div className="bin-rect__seg bin-rect__seg--x" style={{ flex: row.xShare }} />
                            </>
                          ) : null}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
            <div className="bin-chart__ticks">
              {tickRows.map((row) => (
                <span key={row.bin_id} className="bin-chart__tick">
                  {formatDisplayNumber(row.price)}
                </span>
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  );
}

function SeriesChart({ data, dataKey, color }) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={data} margin={{ top: 8, right: 4, left: 0, bottom: 0 }}>
        <CartesianGrid stroke="#141414" vertical={false} />
        <XAxis
          type="number"
          dataKey="bucket_unix_ms"
          domain={["dataMin", "dataMax"]}
          stroke="#4e5563"
          tickLine={false}
          axisLine={false}
          minTickGap={24}
          tickCount={7}
          height={16}
          tickMargin={2}
          tick={{ fill: "#9aa3b2", fontSize: 13 }}
          tickFormatter={timeTickFormatter}
        />
        <YAxis
          stroke="#4e5563"
          tickLine={false}
          axisLine={false}
          width={46}
          tickMargin={4}
          tick={{ fill: "#cdd5e0", fontSize: 13 }}
          tickFormatter={axisTickFormatter}
        />
        <Tooltip
          contentStyle={tooltipStyle}
          labelStyle={tooltipLabelStyle}
          formatter={tooltipValueFormatter}
          labelFormatter={tooltipLabelFormatter}
        />
        <Line type="monotone" dataKey={dataKey} stroke={color} strokeWidth={2} dot={false} activeDot={{ r: 3, fill: color }} />
      </LineChart>
    </ResponsiveContainer>
  );
}

function useAnimatedNumber(targetValue) {
  const numericTarget = Number(targetValue || 0);
  const [displayValue, setDisplayValue] = useState(Number.isFinite(numericTarget) ? numericTarget : 0);
  const frameRef = useRef(0);

  useEffect(() => {
    if (!Number.isFinite(numericTarget)) {
      setDisplayValue(0);
      return undefined;
    }

    cancelAnimationFrame(frameRef.current);

    const tick = () => {
      let done = false;
      setDisplayValue((current) => {
        const diff = numericTarget - current;
        if (Math.abs(diff) < 1) {
          done = true;
          return numericTarget;
        }
        const step = Math.max(1, Math.ceil(Math.abs(diff) / 18));
        const next = current + Math.sign(diff) * step;
        if ((diff > 0 && next >= numericTarget) || (diff < 0 && next <= numericTarget)) {
          done = true;
          return numericTarget;
        }
        return next;
      });

      if (!done) {
        frameRef.current = requestAnimationFrame(tick);
      }
    };

    frameRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frameRef.current);
  }, [numericTarget]);

  return displayValue;
}

function compactEventName(value) {
  if (typeof value !== "string" || value.length <= 18) {
    return value || "-";
  }
  return value.replace("event_cpi::", "cpi::");
}

function seriesForChart(items, key) {
  const HOUR_MS = 3_600_000;
  return items.map((item) => {
    const bucketUnixMsRaw = Number(item.bucket_unix_ms);
    const bucketRaw = Number(item.bucket);
    const bucketUnixMs = Number.isFinite(bucketUnixMsRaw)
      ? bucketUnixMsRaw
      : Number.isFinite(bucketRaw)
        ? bucketRaw * HOUR_MS
        : 0;
    const value = Number(item.value);
    return {
      bucket_unix_ms: bucketUnixMs,
      [key]: Number.isFinite(value) ? value : 0,
    };
  });
}

function axisTickFormatter(value) {
  return fmtCompact(value);
}

function timeTickFormatter(value) {
  const asText = fmtDateTime(value);
  if (asText === "-") {
    return "-";
  }
  return asText.slice(11, 16);
}

function tooltipValueFormatter(value) {
  return [fmtInt(value), "swaps"];
}

function tooltipLabelFormatter(value) {
  return fmtDateTime(value);
}

const tooltipStyle = { background: "#050505", border: "1px solid #1f1f1f", borderRadius: 0, color: "#d7deea", fontSize: 13 };
const tooltipLabelStyle = { color: "#7f8794" };
