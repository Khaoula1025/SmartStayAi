'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import { 
  TrendingUp, 
  RefreshCw, 
  DollarSign, 
  BarChart2, 
  CheckCircle, 
  Activity, 
  ArrowRight, 
  ChevronRight,
  Menu,
  X
} from 'lucide-react';
import { 
  LineChart, 
  Line, 
  ResponsiveContainer, 
  YAxis, 
  XAxis 
} from 'recharts';
import { useAuth } from './context/AuthContext';

// Mock data for the dashboard preview chart
const mockChartData = Array.from({ length: 30 }, (_, i) => ({
  day: i + 1,
  occupancy: Math.floor(65 + Math.random() * 25)
}));

export default function LandingPage() {
  const { user, isLoading } = useAuth();
  const router = useRouter();

  const scrollToFeatures = () => {
    const element = document.getElementById('features');
    element?.scrollIntoView({ behavior: 'smooth' });
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-navy flex items-center justify-center">
        <div className="w-12 h-12 border-4 border-gold border-t-transparent rounded-full animate-spin"></div>
      </div>
    );
  }

  return (
    <div className="min-h-screen font-sans selection:bg-gold selection:text-navy">
      {/* Navigation */}
      <nav className="fixed top-0 w-full z-50 bg-navy/80 backdrop-blur-md border-b border-white/10 h-20">
        <div className="max-w-7xl mx-auto px-6 h-full flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-white font-bold text-xl tracking-tight">SmartStay</span>
            <span className="text-gold font-medium text-xl tracking-tight">Intelligence</span>
          </div>
          <div className="hidden md:flex items-center gap-8">
            <button onClick={scrollToFeatures} className="text-white/80 hover:text-white transition-colors text-sm font-medium">Features</button>
            <Link 
              href={user ? "/dashboard" : "/login"} 
              className="px-5 py-2.5 rounded-lg border border-gold text-gold hover:bg-gold hover:text-navy transition-all duration-300 text-sm font-bold shadow-lg shadow-gold/10"
            >
              {user ? "Dashboard" : "Sign In"}
            </Link>
          </div>
        </div>
      </nav>

      {/* Hero Section */}
      <section className="relative min-h-screen pt-20 bg-navy flex items-center justify-center overflow-hidden">
        {/* Decorative Grid Overlay */}
        <div className="absolute inset-0 opacity-10 pointer-events-none" 
             style={{ backgroundImage: 'radial-gradient(circle at 2px 2px, white 1px, transparent 0)', backgroundSize: '40px 40px' }}></div>
        <div className="absolute top-[-20%] right-[-10%] w-[60%] h-[60%] bg-gold/10 blur-[120px] rounded-full pointer-events-none"></div>
        <div className="absolute bottom-[-20%] left-[-10%] w-[60%] h-[60%] bg-navy-light/30 blur-[120px] rounded-full pointer-events-none"></div>

        <div className="relative z-10 max-w-4xl mx-auto px-6 text-center">
          <div className="inline-flex items-center px-4 py-1.5 rounded-full bg-gold/10 border border-gold/20 text-gold text-xs font-bold uppercase tracking-widest mb-8 animate-fade-in">
            <span className="flex h-2 w-2 rounded-full bg-gold mr-3"></span>
            Powered by UNO Hotels
          </div>
          
          <h1 className="text-5xl md:text-7xl font-bold text-white mb-8 leading-[1.1] tracking-tight">
            Revenue Intelligence for <span className="text-white/90">Modern Hotels</span>
          </h1>
          
          <p className="text-xl text-text-muted max-w-2xl mx-auto mb-12 leading-relaxed">
            AI-powered occupancy forecasting and rate optimization for The Hickstead Hotel. 
            Predict demand, optimize rates, and maximize RevPAR — automatically.
          </p>

          <div className="flex flex-col sm:flex-row items-center justify-center gap-4 mb-16">
            <Link 
              href={user ? "/dashboard" : "/login"} 
              className="w-full sm:w-auto px-8 py-4 bg-gold hover:bg-gold-dark text-navy font-bold rounded-xl transition-all duration-300 transform hover:scale-105 active:scale-95 shadow-xl shadow-gold/20"
            >
              {user ? "Back to Dashboard" : "Sign In to Dashboard"}
            </Link>
            <button 
              onClick={scrollToFeatures}
              className="w-full sm:w-auto px-8 py-4 bg-transparent border-2 border-white/20 hover:border-white text-white font-bold rounded-xl transition-all duration-300"
            >
              View Demo
            </button>
          </div>

          <div className="flex flex-wrap items-center justify-center gap-4">
            <div className="px-5 py-2 rounded-full bg-white/5 border border-white/10 backdrop-blur-sm">
              <span className="text-white font-bold mr-2">88%</span>
              <span className="text-text-muted text-sm">Forecast Accuracy</span>
            </div>
            <div className="px-5 py-2 rounded-full bg-white/5 border border-white/10 backdrop-blur-sm">
              <span className="text-white font-bold mr-2">307</span>
              <span className="text-text-muted text-sm">Days Predicted</span>
            </div>
            <div className="px-5 py-2 rounded-full bg-white/5 border border-white/10 backdrop-blur-sm">
              <span className="text-white font-bold mr-2">Daily</span>
              <span className="text-text-muted text-sm">Auto-Rescore</span>
            </div>
          </div>
        </div>
      </section>

      {/* Stats Bar */}
      <section className="bg-gold py-8 border-y-4 border-navy/10 relative z-20">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-8 md:gap-4 items-center divide-y md:divide-y-0 md:divide-x divide-navy/20">
            <div className="text-center md:border-r border-navy/20">
              <p className="text-3xl font-black text-navy leading-none mb-1">52</p>
              <p className="text-xs font-bold text-navy/70 uppercase tracking-widest">Hotel Rooms</p>
            </div>
            <div className="text-center md:border-r border-navy/20">
              <p className="text-3xl font-black text-navy leading-none mb-1">638</p>
              <p className="text-xs font-bold text-navy/70 uppercase tracking-widest">Training Days</p>
            </div>
            <div className="text-center md:border-r border-navy/20">
              <p className="text-3xl font-black text-navy leading-none mb-1">12.0pp</p>
              <p className="text-xs font-bold text-navy/70 uppercase tracking-widest">Avg MAE</p>
            </div>
            <div className="text-center">
              <p className="text-3xl font-black text-navy leading-none mb-1">GBM + RF</p>
              <p className="text-xs font-bold text-navy/70 uppercase tracking-widest">Model Ensemble</p>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section id="features" className="py-32 bg-white">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-20">
            <h2 className="text-4xl font-bold text-navy mb-4 italic">Everything a Revenue Manager Needs</h2>
            <p className="text-lg text-text-muted max-w-2xl mx-auto">From raw PMS data to actionable rate recommendations — automated.</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
            <FeatureCard 
              icon={TrendingUp}
              title="60-Day Occupancy Forecast"
              desc="GBM + Random Forest ensemble predicts daily occupancy with confidence intervals updated every morning."
            />
            <FeatureCard 
              icon={RefreshCw}
              title="Daily Auto-Rescore"
              desc="Every morning at 06:00 Airflow re-scores the next 60 days using fresh booking-on-books data from the PMS."
            />
            <FeatureCard 
              icon={DollarSign}
              title="Rate Recommendations"
              desc="5-tier dynamic pricing (promotional → premium) based on predicted occupancy, floor price, events and pace."
            />
            <FeatureCard 
              icon={BarChart2}
              title="Pace & BOB Analysis"
              desc="Track booking pace vs same time last year. Know immediately if a date is running ahead or behind budget."
            />
            <FeatureCard 
              icon={CheckCircle}
              title="Rate Decision Audit Trail"
              desc="Accept, override or ignore recommendations. Every decision is logged with timestamp and reason for full accountability."
            />
            <FeatureCard 
              icon={Activity}
              title="Pipeline Monitoring"
              desc="Monitor ETL pipeline status, trigger retraining, and track model accuracy over time from a single interface."
            />
          </div>
        </div>
      </section>

      {/* How it Works Section */}
      <section className="py-32 bg-surface">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-20">
            <h2 className="text-3xl font-bold text-navy">How SmartStay Works</h2>
          </div>

          <div className="flex flex-col lg:flex-row items-center justify-between gap-12 relative">
            <Step 
              num="1"
              title="Data Pipeline"
              desc="Raw PMS extracts are automatically cleaned, validated and merged into a training matrix."
            />
            <div className="hidden lg:block flex-1 h-px bg-gold relative">
              <div className="absolute right-0 top-1/2 -translate-y-1/2 w-2 h-2 rounded-full bg-gold"></div>
            </div>
            <Step 
              num="2"
              title="ML Model"
              desc="GBM + Random Forest ensemble trained on 18 months of historical data predicts occupancy for every date."
            />
            <div className="hidden lg:block flex-1 h-px bg-gold relative">
              <div className="absolute right-0 top-1/2 -translate-y-1/2 w-2 h-2 rounded-full bg-gold"></div>
            </div>
            <Step 
              num="3"
              title="Daily Intelligence"
              desc="Every morning: fresh BOB data rescores the next 60 days. Revenue manager sees updated recommendations."
            />
          </div>
        </div>
      </section>

      {/* Dashboard Preview Section */}
      <section className="py-32 bg-navy overflow-hidden">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-20">
            <h2 className="text-4xl font-bold text-white mb-4">Built for Revenue Managers</h2>
            <p className="text-lg text-text-muted">Not data scientists. Every insight is actionable.</p>
          </div>

          <div className="relative mx-auto max-w-5xl transform scale-90 md:scale-100 rotate-1 shadow-2xl rounded-2xl overflow-hidden border border-white/10">
            {/* Fake Dashboard Shell */}
            <div className="bg-surface flex h-[600px] w-full">
              {/* Sidebar */}
              <div className="w-20 md:w-64 bg-navy border-r border-white/5 flex flex-col p-4">
                <div className="w-8 h-8 bg-gold rounded mb-12"></div>
                <div className="space-y-4">
                  {[1, 2, 3, 4, 5].map(i => (
                    <div key={i} className="h-10 w-full bg-white/5 rounded"></div>
                  ))}
                </div>
              </div>
              
              {/* Content */}
              <div className="flex-1 p-8 space-y-8 overflow-hidden">
                <div className="flex justify-between items-center mb-8">
                  <div className="h-8 w-48 bg-navy/10 rounded"></div>
                  <div className="h-10 w-32 bg-gold/20 rounded"></div>
                </div>

                <div className="grid grid-cols-3 gap-6">
                  {[1, 2, 3].map(i => (
                    <div key={i} className="bg-white p-6 rounded-xl border border-border shadow-sm">
                      <div className="h-4 w-24 bg-navy/5 rounded mb-4"></div>
                      <div className="h-8 w-32 bg-navy/20 rounded"></div>
                    </div>
                  ))}
                </div>

                <div className="bg-white p-6 rounded-xl border border-border flex flex-col h-64">
                   <div className="h-4 w-48 bg-navy/5 rounded mb-6"></div>
                   <div className="flex-1 w-full opacity-60">
                     <ResponsiveContainer width="100%" height="100%">
                       <LineChart data={mockChartData}>
                         <Line type="monotone" dataKey="occupancy" stroke="#C9A84C" strokeWidth={4} dot={false} />
                       </LineChart>
                     </ResponsiveContainer>
                   </div>
                </div>

                <div className="bg-white rounded-xl border border-border overflow-hidden">
                  <div className="p-4 border-b border-border space-y-4">
                    {[1, 2, 3].map(i => (
                      <div key={i} className="flex justify-between items-center">
                        <div className="h-3 w-32 bg-navy/5 rounded"></div>
                        <div className="h-3 w-16 bg-navy/5 rounded"></div>
                        <div className="h-6 w-24 bg-gold/10 rounded-full"></div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>

            {/* Shine effect on overlay */}
            <div className="absolute inset-0 bg-gradient-to-tr from-white/5 to-transparent pointer-events-none"></div>
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-32 bg-navy border-t border-white/10">
        <div className="max-w-4xl mx-auto px-6 text-center">
          <h2 className="text-3xl md:text-5xl font-bold text-white mb-6">Ready to Optimize Revenue at Hickstead?</h2>
          <p className="text-xl text-text-muted mb-12">Sign in to access your live forecast and rate recommendations.</p>
          
          <Link 
            href={user ? "/dashboard" : "/login"} 
            className="inline-flex items-center px-10 py-5 bg-gold hover:bg-gold-dark text-navy font-black rounded-2xl transition-all duration-300 transform hover:scale-105 shadow-2xl shadow-gold/20 uppercase tracking-widest"
          >
            {user ? "Enter Dashboard" : "Access Dashboard"}
            <ArrowRight className="ml-3 w-6 h-6" />
          </Link>
          
          <div className="mt-12 flex items-center justify-center gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-gold"></span>
            <p className="text-sm font-medium text-gold/80 italic">The Hickstead Hotel, West Sussex · UNO Hotels Group</p>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="bg-[#111827] py-12 border-t border-white/5">
        <div className="max-w-7xl mx-auto px-6 flex flex-col md:flex-row justify-between items-center gap-6">
          <div className="text-center md:text-left">
            <p className="text-white font-bold opacity-90">SmartStay Intelligence</p>
            <p className="text-text-muted text-sm mt-1">© 2026 UNO Hotels Group. All rights reserved.</p>
          </div>
          <div className="flex items-center gap-4 text-sm text-text-muted">
             <span className="px-3 py-1 bg-white/5 rounded border border-white/10 uppercase tracking-widest text-[10px]">The Hickstead Hotel</span>
             <span className="hidden md:inline text-white/20">|</span>
             <p>West Sussex · 52 Rooms</p>
          </div>
        </div>
      </footer>
    </div>
  );
}

function FeatureCard({ icon: Icon, title, desc }) {
  return (
    <div className="group bg-white p-8 rounded-2xl border border-border shadow-sm hover:shadow-xl transition-all duration-300 hover:-translate-y-1 relative overflow-hidden">
      <div className="absolute top-0 left-0 w-full h-1 bg-gold scale-x-0 group-hover:scale-x-100 transition-transform origin-left duration-500"></div>
      <div className="mb-6 p-3 bg-navy/5 text-gold rounded-xl w-fit group-hover:bg-navy group-hover:text-white transition-colors duration-300">
        <Icon className="w-6 h-6" />
      </div>
      <h3 className="text-xl font-bold text-navy mb-4">{title}</h3>
      <p className="text-text-muted leading-relaxed text-sm">{desc}</p>
    </div>
  );
}

function Step({ num, title, desc }) {
  return (
    <div className="flex-1 flex flex-col items-center text-center max-w-xs">
      <div className="w-16 h-16 rounded-full bg-navy border-4 border-gold flex items-center justify-center text-gold font-black text-2xl shadow-xl mb-6 transform transition-transform hover:scale-110">
        {num}
      </div>
      <h3 className="text-lg font-bold text-navy mb-3 uppercase tracking-wider">{title}</h3>
      <p className="text-sm text-text-muted leading-relaxed">{desc}</p>
    </div>
  );
}
