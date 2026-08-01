# Design prototypes

Standalone HTML mockups. Not wired into the app — open them in a browser.

- `prototype.html` — full editor redesign: content-forward segment list,
  source above target, QA rail. Structure only; no Monaco, no schemas.
- `welcome-b.html` — welcome screen, typographic direction. This one is
  already ported into the app (`styles/13-welcome-screen.css`).

Screenshot one with:

    npx electron <scratchpad>/shot-design.js design/prototype.html out.png [--light]
