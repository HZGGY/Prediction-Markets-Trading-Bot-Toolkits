import { TELEGRAM_URL } from '../bots';
import { useT } from '../messages';

export function ManagedSection() {
  const t = useT();
  const m = t.managed;

  return (
    <section id="managed" className="py-24 border-t border-border-subtle">
      <div className="container-x">
        <div className="max-w-3xl mb-10">
          <div className="text-sm font-semibold text-purple-400 uppercase tracking-wider mb-3">{m.eyebrow}</div>
          <h2 className="text-4xl md:text-5xl font-bold tracking-tight mb-5">{m.headline}</h2>
          <p className="text-lg text-zinc-400 leading-relaxed">{m.description}</p>
        </div>

        {/* By the numbers — honest, verifiable signals */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-10">
          {m.stats.map((s) => (
            <div key={s.label} className="card p-5 text-center">
              <div className="text-2xl md:text-3xl font-bold text-white">{s.value}</div>
              <div className="mt-1 text-xs uppercase tracking-wider text-zinc-500">{s.label}</div>
            </div>
          ))}
        </div>

        {/* Paper-beta honesty callout */}
        <div className="max-w-3xl mb-14 rounded-xl border border-amber-500/25 bg-amber-500/5 px-5 py-4 text-sm text-amber-200/90 leading-relaxed">
          {m.statusNote}
        </div>

        {/* What you get */}
        <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-5">{m.featuresTitle}</h3>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-16">
          {m.features.map((f) => (
            <div key={f.title} className="card p-5 flex gap-4">
              <div className="text-2xl shrink-0" aria-hidden>{f.icon}</div>
              <div>
                <div className="font-bold text-white mb-1">{f.title}</div>
                <div className="text-sm text-zinc-400 leading-relaxed">{f.body}</div>
              </div>
            </div>
          ))}
        </div>

        {/* Plans */}
        <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-5">{m.plansTitle}</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {m.plans.map((p) => (
            <div
              key={p.name}
              className={`card p-6 flex flex-col ${
                p.highlight ? 'border-purple-500/40 ring-1 ring-purple-500/20' : ''
              }`}
            >
              <div className="font-bold text-white text-lg">{p.name}</div>
              <div className="mt-2 text-2xl font-bold text-white">{p.price}</div>
              <div className="mt-1 text-sm text-zinc-400">{p.fee}</div>
              <div className="mt-4 pt-4 border-t border-border-subtle text-sm text-zinc-400 leading-relaxed">{p.forWho}</div>
            </div>
          ))}
        </div>
        <p className="mt-5 text-sm text-zinc-500 max-w-3xl leading-relaxed">{m.plansNote}</p>

        <div className="mt-10">
          <a href={TELEGRAM_URL} target="_blank" rel="noreferrer" className="btn-primary text-base px-7 py-3.5">
            <TelegramIcon />
            <span>{m.cta}</span>
          </a>
        </div>
      </div>
    </section>
  );
}

function TelegramIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" aria-hidden>
      <path d="M9.78 18.65l.28-4.23 7.68-6.92c.34-.31-.07-.46-.52-.19L7.74 13.3 3.64 12c-.88-.25-.89-.86.2-1.3l15.97-6.16c.73-.33 1.43.18 1.15 1.3l-2.72 12.81c-.19.91-.74 1.13-1.5.71L12.6 16.3l-1.99 1.93c-.23.23-.42.42-.83.42z"/>
    </svg>
  );
}
