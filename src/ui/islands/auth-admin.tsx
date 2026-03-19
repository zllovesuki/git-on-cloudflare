/// <reference lib="dom" />

import { useEffect, useMemo, useState } from "react";

import { hydrateIsland } from "@/ui/client/hydrate";

type AuthUser = {
  owner: string;
  tokens?: string[];
};

type UsersResponse = {
  users?: AuthUser[];
};

type MessageState = {
  text: string;
  tone: "success" | "error";
} | null;

export type AuthAdminProps = Record<string, never>;

function messageClassName(message: MessageState): string {
  if (!message) {
    return "hidden mb-4 rounded-xl p-4";
  }

  return message.tone === "error"
    ? "block mb-4 rounded-xl bg-red-50 p-4 text-red-800 dark:bg-red-900/20 dark:text-red-200"
    : "block mb-4 rounded-xl bg-green-50 p-4 text-green-800 dark:bg-green-900/20 dark:text-green-200";
}

async function readErrorText(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return "Request failed";
  }
}

export function AuthAdminIsland(_props: AuthAdminProps) {
  const [adminToken, setAdminToken] = useState("");
  const [owner, setOwner] = useState("");
  const [token, setToken] = useState("");
  const [users, setUsers] = useState<AuthUser[]>([]);
  const [loadingUsers, setLoadingUsers] = useState(false);
  const [adding, setAdding] = useState(false);
  const [message, setMessage] = useState<MessageState>(null);

  useEffect(() => {
    try {
      const saved = localStorage.getItem("adminToken");
      if (saved) {
        setAdminToken(saved);
      }
    } catch {}
  }, []);

  useEffect(() => {
    if (!message) {
      return;
    }

    const timeout = window.setTimeout(() => {
      setMessage(null);
    }, 4000);
    return () => window.clearTimeout(timeout);
  }, [message]);

  const ownerCountLabel = useMemo(
    () => `${users.length} ${users.length === 1 ? "owner" : "owners"}`,
    [users.length]
  );

  async function callApi(path: string, init: RequestInit = {}) {
    const headers = new Headers(init.headers || {});
    headers.set("Authorization", `Bearer ${adminToken.trim()}`);

    const response = await fetch(`/auth/api${path}`, { ...init, headers });
    if (!response.ok) {
      setMessage({
        text: `Error ${response.status}: ${await readErrorText(response)}`,
        tone: "error",
      });
      return null;
    }

    return response.json();
  }

  async function loadUsers() {
    setLoadingUsers(true);
    const data = (await callApi("/users")) as UsersResponse | null;
    setLoadingUsers(false);
    if (!data) {
      return;
    }

    const nextUsers = data.users || [];
    setUsers(nextUsers);
    if (!nextUsers.length) {
      setMessage({ text: "No owners found", tone: "success" });
    }
  }

  async function addOwner() {
    const nextOwner = owner.trim();
    const nextToken = token.trim();
    if (!nextOwner || !nextToken) {
      setMessage({ text: "Owner and token required", tone: "error" });
      return;
    }

    setAdding(true);
    const result = await callApi("/users", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ owner: nextOwner, token: nextToken }),
    });
    setAdding(false);

    if (!result) {
      return;
    }

    setMessage({ text: `Owner '${nextOwner}' and token added successfully`, tone: "success" });
    setToken("");
    await loadUsers();
  }

  async function deleteOwner(targetOwner: string) {
    if (!window.confirm(`Delete ${targetOwner}?`)) {
      return;
    }

    const result = await callApi("/users", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ owner: targetOwner }),
    });

    if (!result) {
      return;
    }

    setMessage({ text: `Owner '${targetOwner}' deleted successfully`, tone: "success" });
    await loadUsers();
  }

  async function deleteToken(targetOwner: string, tokenHash: string) {
    if (!window.confirm(`Delete token ${tokenHash.slice(0, 8)}... for ${targetOwner}?`)) {
      return;
    }

    const result = await callApi("/users", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ owner: targetOwner, tokenHash }),
    });

    if (!result) {
      return;
    }

    setMessage({ text: `Token deleted successfully for '${targetOwner}'`, tone: "success" });
    await loadUsers();
  }

  return (
    <div className="animate-slide-up">
      <header className="page-header">
        <div>
          <span className="mb-1 inline-block text-xs font-semibold uppercase tracking-wider text-accent-500 dark:text-accent-400">
            Settings
          </span>
          <h2 className="m-0">Auth Management</h2>
        </div>
        <div></div>
      </header>
      <p className="muted mb-6">
        Use your root admin token to manage owners and tokens. Tokens are stored as SHA-256 hashes.
        You can add and delete owners and remove individual token hashes.
      </p>
      <div className={messageClassName(message)} aria-live="polite" role="status">
        {message?.text}
      </div>
      <div className="mb-6 grid gap-6 md:grid-cols-2">
        <div className="card p-6">
          <h3>1) Admin Token</h3>
          <div className="space-y-3">
            <input
              type="password"
              placeholder="Enter AUTH_ADMIN_TOKEN"
              value={adminToken}
              onChange={(event) => {
                const nextValue = event.target.value;
                setAdminToken(nextValue);
                try {
                  localStorage.setItem("adminToken", nextValue);
                } catch {}
              }}
              className="w-full rounded-xl border border-zinc-300 bg-white px-3 py-2 focus:outline-hidden focus:ring-2 focus:ring-accent-500 dark:border-zinc-700 dark:bg-zinc-800"
            />
            <button className="btn w-full sm:w-auto" type="button" onClick={() => void loadUsers()}>
              {loadingUsers ? "Loading..." : "Load Users"}
            </button>
          </div>
          <div className="muted mt-3 text-sm">
            Your browser will remember this for the current tab.
          </div>
        </div>
        <div className="card p-6">
          <h3>2) Add Owner / Token</h3>
          <div className="space-y-3">
            <input
              placeholder="owner (e.g., rachel)"
              value={owner}
              onChange={(event) => setOwner(event.target.value)}
              className="w-full rounded-xl border border-zinc-300 bg-white px-3 py-2 focus:outline-hidden focus:ring-2 focus:ring-accent-500 dark:border-zinc-700 dark:bg-zinc-800"
            />
            <input
              placeholder="token (raw)"
              value={token}
              onChange={(event) => setToken(event.target.value)}
              className="w-full rounded-xl border border-zinc-300 bg-white px-3 py-2 focus:outline-hidden focus:ring-2 focus:ring-accent-500 dark:border-zinc-700 dark:bg-zinc-800"
            />
            <button
              className="btn w-full sm:w-auto"
              type="button"
              onClick={() => void addOwner()}
              disabled={adding}
            >
              {adding ? "Adding..." : "Add"}
            </button>
          </div>
          <div className="muted mt-3 text-sm">
            Tip: Each collaborator uses their own token. For Git Basic auth, set username = owner
            and password = token.
          </div>
        </div>
      </div>
      <div className="card mb-6 p-6">
        <div className="mb-4 flex items-center justify-between">
          <h3>Owners</h3>
          <span className="rounded-full bg-zinc-200 px-3 py-1 text-sm dark:bg-zinc-700">
            {ownerCountLabel}
          </span>
        </div>
        {users.length ? (
          <div className="space-y-4">
            {users.map((user) => (
              <div
                key={user.owner}
                className="flex items-start justify-between rounded-xl border border-zinc-200 bg-white p-4 dark:border-zinc-800/60 dark:bg-zinc-800"
              >
                <div>
                  <div className="text-lg font-semibold">{user.owner}</div>
                  <div className="mb-2 text-sm text-zinc-500 dark:text-zinc-400">
                    tokens: {(user.tokens || []).length}
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {(user.tokens || []).length ? (
                      (user.tokens || []).map((hash) => (
                        <span
                          key={hash}
                          className="inline-flex items-center gap-1 rounded-lg bg-zinc-100 px-2 py-1 dark:bg-zinc-700"
                        >
                          <code className="text-sm">{String(hash).slice(0, 8)}...</code>
                          <button
                            className="font-bold text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300"
                            title="Delete token"
                            type="button"
                            onClick={() => void deleteToken(user.owner, hash)}
                          >
                            x
                          </button>
                        </span>
                      ))
                    ) : (
                      <span className="text-zinc-500 dark:text-zinc-400">(no tokens)</span>
                    )}
                  </div>
                </div>
                <div>
                  <button
                    className="btn secondary"
                    type="button"
                    onClick={() => void deleteOwner(user.owner)}
                  >
                    Delete Owner
                  </button>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-zinc-500 dark:text-zinc-400">
            {loadingUsers ? "Loading..." : "(none)"}
          </div>
        )}
      </div>
      <div className="rounded-2xl bg-accent-50 p-6 dark:bg-accent-900/20">
        <h3 className="mb-3 text-lg font-semibold">Usage examples</h3>
        <ul className="space-y-2 text-zinc-600 dark:text-zinc-400">
          <li>
            Git Basic auth: use <code>username = owner</code>, <code>password = token</code>.
          </li>
        </ul>
      </div>
    </div>
  );
}

export function initAuthAdmin() {
  hydrateIsland<AuthAdminProps>("auth-admin", AuthAdminIsland);
}
