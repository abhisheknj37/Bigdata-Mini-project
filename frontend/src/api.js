import axios from 'axios';

const api = axios.create({
  baseURL: `${import.meta.env.VITE_API_URL || 'http://127.0.0.1:5000'}/api`,
});

api.interceptors.request.use(config => {
  const token = localStorage.getItem('mw_token');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

api.interceptors.response.use(
  response => response,
  error => {
    if (error.response?.status === 401) {
      localStorage.removeItem('mw_token');
      localStorage.removeItem('mw_user');
      if (window.location.pathname !== '/') window.location.href = '/';
    }
    return Promise.reject(error);
  },
);

export default api;
