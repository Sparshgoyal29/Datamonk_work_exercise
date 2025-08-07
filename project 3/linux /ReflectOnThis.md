# Reflect on Linux Permissions

This document helps you think more deeply about how Linux file and directory permissions work, their practical uses, and why handling them correctly is important for system security.

---

## 🔐 File vs. Directory Permissions

In Linux, the **execute (x)** permission means different things for files and directories:

- For **files**, `x` means you can **run the file** as a program or script.
- For **directories**, `x` means you can **enter or access the directory**.

> 📌 Even if you have read (`r`) permission on the files inside a folder, you **can’t open or view them** unless you also have execute (`x`) permission on the directory itself. That’s why `x` is essential for navigating into directories.

---

## ⚠️ The Danger of 777 Permissions

Setting permissions to **777 (rwxrwxrwx)** gives **everyone full access** — read, write, and execute.

### 🔥 Example: Security Risk on a Web Server

If you give a web-accessible script (like `upload.php`) 777 permissions:

- Anyone on the internet could **upload or run malicious files**.
- An attacker could replace the file with harmful code.
- This could allow full access to your website or even the server.

> ❗ Always give **only the permissions needed**, nothing more.

---

## 🔢 Symbolic (`chmod g+x`) vs. Octal (`chmod 765`) Permissions

You have a file with these permissions: `-rwx-w-r--`.

Now you want to **only add execute permission for the group**.

- Using **symbolic mode**:  
  `chmod g+x a_file.txt`  
  ✅ Adds execute permission for group only — safe and clear.

- Using **octal mode**:  
  You’d have to figure out the new number manually (which could be error-prone)  
  ❌ Risk: You might accidentally overwrite other permissions.

> ✔️ Symbolic mode is safer when making small changes to permissions.

---

## 🧨 The Power and Risk of `sudo`

The `sudo` command gives you **root (admin) access**, which can be very dangerous if used incorrectly.

### 😨 Example: A Small Typo, Big Problem

You meant to delete a folder:

```bash
sudo rm -rf ./temp_files/
