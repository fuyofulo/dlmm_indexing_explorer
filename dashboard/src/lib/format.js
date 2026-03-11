export function fmtDateTime(ms) {
  const value = Number(ms);
  if (!Number.isFinite(value) || value <= 0) {
    return "-";
  }
  return new Date(value).toLocaleString("en-US", { hour12: false });
}

export function fmtCompact(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return String(value ?? "-");
  }
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(parsed);
}

export function fmtInt(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return String(value ?? "-");
  }
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(parsed);
}

export function shortKey(value, head = 6, tail = 6) {
  if (typeof value !== "string" || value.length <= head + tail + 1) {
    return value || "-";
  }
  return `${value.slice(0, head)}...${value.slice(-tail)}`;
}

export function valueOrDash(value) {
  if (value === undefined || value === null || value === "") {
    return "-";
  }
  return String(value);
}

export function formatDisplayNumber(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return valueOrDash(value);
  }
  if (Math.abs(parsed) >= 1_000_000) {
    return fmtCompact(parsed);
  }
  if (Math.abs(parsed) >= 1) {
    return parsed.toLocaleString("en-US", { maximumFractionDigits: 4 });
  }
  return parsed.toLocaleString("en-US", { maximumSignificantDigits: 8 });
}

export function formatPercent(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return valueOrDash(value);
  }
  return `${parsed.toLocaleString("en-US", { maximumFractionDigits: 6 })}%`;
}

export function formatPriceStep(binStep) {
  const parsed = Number(binStep);
  if (!Number.isFinite(parsed)) {
    return valueOrDash(binStep);
  }
  return `${((parsed / 10_000) * 100).toLocaleString("en-US", { maximumFractionDigits: 4 })}%`;
}

export function windowLabel(minutes) {
  if (minutes < 60) {
    return `${minutes}m`;
  }
  if (minutes < 1440) {
    return `${Math.round(minutes / 60)}h`;
  }
  return `${Math.round(minutes / 1440)}d`;
}
