'use client';

import { usePathname } from 'next/navigation';
import { MapPin, ShieldCheck } from 'lucide-react';

export default function Header() {
  const pathname = usePathname();

  const getTitle = () => {
    if (pathname.startsWith('/dashboard')) return 'Dashboard Overview';
    if (pathname.startsWith('/forecast')) return 'Forecast & Demand';
    if (pathname.startsWith('/rates')) return 'Rate Decisions';
    if (pathname.startsWith('/analytics')) return 'Performance Analytics';
    if (pathname.startsWith('/pipeline')) return 'Pipeline Diagnostics';
    return 'Dashboard';
  };

  const currentDate = new Date().toLocaleDateString('en-GB', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  });

  return (
    <header className="h-[64px] bg-white border-b border-border flex items-center justify-between px-6 sticky top-0 z-10 shadow-sm">
      <div className="flex-1">
        <h2 className="text-xl font-bold text-navy">{getTitle()}</h2>
      </div>

      <div className="flex-1 flex justify-center items-center text-text-muted">
        <MapPin className="w-4 h-4 mr-2" />
        <span className="font-medium text-sm">The Hickstead Hotel</span>
      </div>

      <div className="flex-1 flex justify-end items-center space-x-4">
        <span className="text-sm font-medium text-text-muted">{currentDate}</span>
        <div className="flex items-center px-3 py-1 bg-success/10 text-success rounded-full text-xs font-bold ring-1 ring-inset ring-success/20">
          <ShieldCheck className="w-3 h-3 mr-1" />
          BOB Quality Good
        </div>
      </div>
    </header>
  );
}
