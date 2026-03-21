export const BASE_URL = 'http://localhost:8000/api/v1';

async function fetchWithAuth(url, options = {}) {
  const finalOptions = {
    ...options,
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  };
  
  const response = await fetch(`${BASE_URL}${url}`, finalOptions);
  
  if (response.status === 401) {
    // Redirection should be handled by components/layouts (e.g. DashboardLayout)
    // letting public pages like the landing page check session without being redirected.
  }
  
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new Error(errorData.detail || `API error: ${response.status}`);
  }
  
  if (response.status === 204) {
    return null;
  }
  
  return response.json();
}

export async function login(username, password) {
  return fetchWithAuth('/auth/login', {
    method: 'POST',
    body: JSON.stringify({ identifier: username, password }),
  });
}

export async function register(username, email, password) {
  return fetchWithAuth('/auth/signUp', {
    method: 'POST',
    body: JSON.stringify({ username, email, password }),
  });
}

export async function logout() {
  return fetchWithAuth('/auth/logout', { method: 'POST' });
}

export async function checkSession() {
  // Use the defined verifyToken endpoint in the backend auth router
  return fetchWithAuth('/auth/verifyToken', { method: 'GET' });
}

export async function getDashboardSummary() {
  return fetchWithAuth('/dashboard/summary', { method: 'GET' });
}

export async function getPredictions(dateFrom, dateTo, rateTier = null) {
  let query = `/predictions/?date_from=${dateFrom}&date_to=${dateTo}`;
  if (rateTier && rateTier !== 'all') {
    query += `&rate_tier=${rateTier}`;
  }
  return fetchWithAuth(query, { method: 'GET' });
}

export async function getPredictionByDate(date) {
  return fetchWithAuth(`/predictions/${date}`, { method: 'GET' });
}

export async function getActuals(dateFrom, dateTo) {
  return fetchWithAuth(`/actuals/?date_from=${dateFrom}&date_to=${dateTo}`, { method: 'GET' });
}

export async function getAccuracy(dateFrom, dateTo) {
  return fetchWithAuth(`/analytics/accuracy?date_from=${dateFrom}&date_to=${dateTo}`, { method: 'GET' });
}

export async function getModelMetrics() {
  return fetchWithAuth('/analytics/model/metrics', { method: 'GET' });
}

export async function getModelHistory() {
  return fetchWithAuth('/analytics/model/history', { method: 'GET' });
}

export async function postRateDecision(payload) {
  return fetchWithAuth('/rates/decide', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function getRateDecisions(dateFrom, dateTo) {
  return fetchWithAuth(`/rates/decisions?date_from=${dateFrom}&date_to=${dateTo}`, { method: 'GET' });
}

export async function getPipelineStatus() {
  return fetchWithAuth('/pipeline/status', { method: 'GET' });
}

export async function triggerPipeline(steps) {
  return fetchWithAuth(`/pipeline/trigger?steps=${steps}`, { method: 'POST' });
}

export async function getShapSummary() {
  return fetchWithAuth('/explain/summary', { method: 'GET' });
}

export async function getShapExplanation(date) {
  return fetchWithAuth(`/explain/${date}`, { method: 'GET' });
}

export async function getSeasonality() {
  return fetchWithAuth('/analytics/seasonality/', { method: 'GET' });
}

export async function getSentimentSummary() {
  return fetchWithAuth('/sentiment/summary', { method: 'GET' });
}
