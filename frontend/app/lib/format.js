export function formatOcc(value) {
  if (value === null || value === undefined) return '-';
  const val = value > 1 ? value : value * 100;
  return `${val.toFixed(1)}%`;
}

export function formatRate(value) {
  if (value === null || value === undefined) return '-';
  return new Intl.NumberFormat('en-GB', {
    style: 'currency',
    currency: 'GBP',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

export function formatCurrency(amount) {
  if (amount === null || amount === undefined) return '-';
  return new Intl.NumberFormat('en-GB', {
    style: 'currency',
    currency: 'GBP',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(amount);
}

export function formatPercent(value) {
  return formatOcc(value);
}

export function formatDate(dateString) {
  const options = { weekday: 'short', year: 'numeric', month: 'short', day: 'numeric' };
  const d = new Date(dateString);
  if (isNaN(d.getTime())) return dateString;
  return d.toLocaleDateString('en-GB', options);
}

export function formatShortDate(dateString) {
  const d = new Date(dateString);
  if (isNaN(d.getTime())) return dateString;
  return d.toLocaleDateString('en-GB', {
    month: 'short',
    day: 'numeric'
  });
}

export function formatError(value) {
  if (value === null || value === undefined) return '-';
  return `${Number(value).toFixed(1)}pp`;
}

export function tierColor(tier) {
  switch (tier?.toLowerCase()) {
    case 'promotional': return '#718096';
    case 'value': return '#4299E1';
    case 'standard': return '#38A169';
    case 'high': return '#D69E2E';
    case 'premium': return '#E53E3E';
    default: return '#718096';
  }
}

export function qualityVariant(q) {
  switch (q?.toLowerCase()) {
    case 'high': return 'success';
    case 'medium': return 'warning';
    case 'low': return 'danger';
    default: return 'muted';
  }
}
