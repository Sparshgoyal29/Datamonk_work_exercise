# Reflection: Git Internals Project

## What did you learn?

- Git stores files as **blobs** (content-only objects)
- A **tree** stores file structure, referencing blobs and subtrees
- A **commit** points to a tree and stores metadata
- Commands like `git cat-file` reveal Git’s internal database

## Why is this important?

Understanding Git internals makes it easier to:
- Debug strange Git issues
- Appreciate its efficiency
- Work confidently with advanced tools (e.g., rebase, cherry-pick)

## Interesting Facts:

- Even empty files have a hash!
- Folders are stored as trees recursively
- Git is more like a **key-value store** than a file tracker
