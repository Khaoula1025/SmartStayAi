'use client';

import { useEffect, useState } from 'react';
import { X } from 'lucide-react';

export default function Modal({ isOpen, onClose, title, children, footer }) {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = 'unset';
    }

    const handleEscape = (e) => {
      if (e.key === 'Escape') onClose();
    };

    document.addEventListener('keydown', handleEscape);
    return () => {
      document.body.style.overflow = 'unset';
      document.removeEventListener('keydown', handleEscape);
    };
  }, [isOpen, onClose]);

  if (!mounted || !isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center pt-10 pb-10 px-4 sm:p-0">
      <div 
        className="fixed inset-0 bg-navy/80 backdrop-blur-sm transition-opacity" 
        onClick={onClose}
        aria-hidden="true"
      />

      <div className="relative bg-white rounded-xl shadow-2xl w-full max-w-lg overflow-hidden transform transition-all animate-in fade-in zoom-in-95 duration-200">
        <div className="bg-navy px-6 py-4 flex items-center justify-between">
          <h3 className="text-xl font-bold text-white tracking-wide">
            {title}
          </h3>
          <button
            onClick={onClose}
            className="text-white hover:text-gold transition-colors focus:outline-none focus:ring-2 focus:ring-gold rounded p-1"
          >
            <span className="sr-only">Close</span>
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6">
          {children}
        </div>

        {footer && (
          <div className="bg-surface px-6 py-4 border-t border-border flex justify-end space-x-3">
            {footer}
          </div>
        )}
      </div>
    </div>
  );
}
