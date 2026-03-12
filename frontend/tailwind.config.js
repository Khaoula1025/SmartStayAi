/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        navy: "var(--navy)",
        "navy-light": "var(--navy-light)",
        gold: "var(--gold)",
        "gold-light": "var(--gold-light)",
        "gold-dark": "var(--gold-dark)",
        "gold-darker": "var(--gold-darker)",
        surface: "var(--surface)",
        border: "var(--border)",
        "text-dark": "var(--text-dark)",
        "text-muted": "var(--text-muted)",
        success: "var(--success)",
        warning: "var(--warning)",
        danger: "var(--danger)",
      },
    },
  },
  plugins: [],
};
