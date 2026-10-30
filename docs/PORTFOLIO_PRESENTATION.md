# Repository presentation decisions

The repository is structured for a reviewer to understand the problem, see the
specific failure and reproduce the result without first reading a design manual.

## References reviewed

- [Svix](https://github.com/svix/svix-webhooks): concise purpose, deployment options,
  clear configuration and operational boundaries.
- [Hookdeck Outpost](https://github.com/hookdeck/outpost): developer-facing problem,
  quickstart and dedicated documentation for deeper topics.
- [GitHub README guidance](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes): usefulness, getting started and navigation, with relative links.
- [Repository topics](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/classifying-your-repository-with-topics): use concrete purpose/language tags.
- [Social preview](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/customizing-your-repositorys-social-media-preview): the prepared PNG is 1280×640, under 1 MB.

These are presentation references. EventHub does not claim Svix/Outpost production
capabilities or use their names, branding or market adoption as its own evidence.

## Applied to this project

- Russian reviewer overview with an English companion; one concrete A/B incident
  before the technology inventory.
- Actual screenshots and a sanitized machine-readable replay result.
- Docker quickstart using only a standard-library host launcher; Python application
  dependencies live in the container.
- Architecture diagram, code-review entry points and linked verification evidence.
- Real GitHub Actions checks rather than a manually painted success badge.
- MIT license, release notes, security/contribution guidance and issue form.
- Curated public source: no credentials, dumps, private access files, local logs,
  machine-specific mentoring history or virtual environment.
- No artificial commit dates, invented production metrics or fabricated contributors.

`assets/social-preview.png` is a prepared share image. Adding a file does not
configure GitHub's custom social-preview setting; the README uses the matching
SVG directly. The repository link remains the primary portfolio link.
