# Reflect on Linux Tools & Data Management

Use these questions to think more critically about how you manage software and work with data in a Linux environment. This will help you better understand the strengths and trade-offs of common tools used by developers.

---

## 📦 Package Managers: APT vs. Snap

APT and Snap are both tools to install software on Linux, but they work differently.

### Why choose **Snap** over APT?

- Snap packages are **sandboxed** (isolated from the system), which improves security.
- They often include all dependencies, making them easier to run across systems.
- Snap apps receive **automatic updates** and are often maintained directly by vendors.

### Trade-offs of Snap:

- **Slower startup time** due to sandboxing.
- **Larger disk space usage** since dependencies aren’t shared.
- Some Snap apps may feel less integrated with your system than APT versions.

> ✅ Use Snap when you want a secure, self-contained, or newer version of an app.  
> ⚠️ Use APT for system tools and lighter, faster installations.

---

## 🔍 Code Searching: `grep` vs. `ripgrep (rg)`

For searching inside codebases:

- `grep` is powerful and widely used.
- `ripgrep (rg)` is a newer tool **built for speed and efficiency**.

### Why `rg` is better for large projects:

- It’s **faster** — uses Rust and ignores binary files automatically.
- **Respects .gitignore**, so it skips unnecessary files.
- Designed to search code — cleaner and easier to use with large projects.

> 🚀 Use `rg` when working with large projects for a faster, smarter search experience.

---

## 🌐 Internet Tools: `curl` vs. `wget`

Both `curl` and `wget` let you download data from the internet — but they shine in different scenarios.

### When to use `curl`:

- Making **API requests** (like POST, PUT, GET with custom headers).
- Example: Sending JSON data to a server or testing APIs.

### When to use `wget`:

- Downloading **entire files or websites**.
- Can **download recursively**, resume downloads, and mirror sites.

### Key design difference:

- `curl` is built for **data transfer** — especially with APIs and custom requests.
- `wget` is designed for **retrieving files and sites** easily and reliably.

> 💡 Choose the tool that fits your task — APIs? Use `curl`. Full site download? Use `wget`.

---

## 🔗 The Power of Piping: `curl | jq`

Chaining commands like this:

```bash
curl ... | jq '.[0].title'
