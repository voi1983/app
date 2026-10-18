// Переклади. Український текст береться з index.html, тут лише англійська версія.
const EN = {
  "initials": "NS",
  "name": "Name Surname",
  "role": "BAS Developer & Integrator",
  "nav.services": "Services",
  "nav.about": "About",
  "nav.cases": "Cases",
  "nav.process": "Process",
  "nav.contact": "Contact",
  "hero.hello": "// hi, my name is",
  "hero.lead": "I automate accounting and sales in BAS, connect it with other services and take routine work off your team.",
  "hero.cta": "Discuss your task",
  "hero.cases": "See cases",
  "services.title": "Services",
  "s1.t": "BAS development & customization",
  "s1.d": "Setting up and extending BAS configurations (Accounting, Trade Management, ERP) to fit your business processes.",
  "s2.t": "CRM built on BAS",
  "s2.d": "Clients, deals and sales tracked in one system together with accounting, no extra services needed.",
  "s3.t": "Accounting automation",
  "s3.d": "Reports, data processors and document flows that take routine work off your accountant.",
  "s4.t": "API integrations",
  "s4.d": "Data exchange between BAS and websites, marketplaces, banks, delivery services and other systems.",
  "s5.t": "Gmail ↔ BAS",
  "s5.d": "Emails, attachments and orders flow into BAS automatically, and documents from BAS go out to clients by email.",
  "s6.t": "AI integration",
  "s6.d": "Document recognition, data analysis and smart assistants right inside BAS.",
  "s7.t": "Custom solutions for niche tasks",
  "s7.d": "Dedicated tools for specific problems, such as automatically merging supplier price lists from Google Drive.",
  "about.title": "About me",
  "about.p1": "[Placeholder] N+ years working with BAS and 1C: from small customizations to implementing accounting from scratch.",
  "about.p2": "[Placeholder] Worked with companies in retail, manufacturing and services. I also write Python when a task goes beyond BAS.",
  "cases.title": "Cases",
  "label.task": "Task:",
  "label.done": "Solution:",
  "c1.t": "Supplier price list consolidation",
  "c1.task": "price lists arrive in different formats, and merging them by hand takes a long time.",
  "c1.done": "a script pulls Excel files from a Google Drive folder, recognizes models, sizes and prices, and builds a single table.",
  "c2.t": "[Placeholder] Case title",
  "c2.task": "client's task description.",
  "c2.done": "what was done and the result, ideally in numbers.",
  "c3.t": "[Placeholder] Case title",
  "c3.task": "client's task description.",
  "c3.done": "what was done and the result, ideally in numbers.",
  "process.title": "How I work",
  "p1.t": "Request",
  "p1.d": "You describe the task via Telegram or email.",
  "p2.t": "Estimate",
  "p2.d": "I clarify the details and give you a timeline and price.",
  "p3.t": "Development",
  "p3.d": "I build and test on a copy of your database and show progress along the way.",
  "p4.t": "Delivery & support",
  "p4.d": "I move it to production, train your team and support you after launch.",
  "contact.title": "Contact",
  "contact.lead": "Have a task? Write to me and I'll reply within one business day.",
  "contact.tg": "Message on Telegram",
  "contact.mail": "Send an email"
};

const nodes = document.querySelectorAll("[data-i18n]");
const UK = {};
nodes.forEach(el => { UK[el.dataset.i18n] = el.textContent; });
const ukTitle = document.title;

function setLang(lang) {
  const dict = lang === "en" ? EN : UK;
  nodes.forEach(el => {
    const text = dict[el.dataset.i18n];
    if (text !== undefined) el.textContent = text;
  });
  document.documentElement.lang = lang;
  document.title = lang === "en" ? `${EN.name} — ${EN.role}` : ukTitle;
  document.getElementById("lang-toggle").textContent = lang === "en" ? "UA" : "EN";
  try { localStorage.setItem("lang", lang); } catch (e) {}
}

let saved = null;
try { saved = localStorage.getItem("lang"); } catch (e) {}
let current = saved || (navigator.language.startsWith("en") ? "en" : "uk");
setLang(current);

document.getElementById("lang-toggle").addEventListener("click", () => {
  current = current === "en" ? "uk" : "en";
  setLang(current);
});

document.getElementById("year").textContent = new Date().getFullYear();
