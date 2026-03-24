/* auth.js — copy from another Dreck Suite repo (e.g. Restaurant-tracker) */
/* This file should be replaced with the shared auth.js from businessden org */

(function() {
    const VALID_USERS = {
        'admin': 'B1zD3n',
        'justin': 'B1zD3n',
        'matt': 'B1zD3n',
        'max': 'B1zD3n',
        'thomas': 'B1zD3n',
        'aaron': 'B1zD3n'
    };

    const SESSION_KEY = 'dreck_session';
    const SESSION_DAYS = 30;

    function checkSession() {
        const session = localStorage.getItem(SESSION_KEY);
        if (!session) return false;
        try {
            const data = JSON.parse(session);
            if (Date.now() - data.ts < SESSION_DAYS * 86400000) return true;
        } catch(e) {}
        return false;
    }

    function showLogin() {
        document.body.innerHTML = '';
        const overlay = document.createElement('div');
        overlay.style.cssText = 'position:fixed;inset:0;background:#08090a;display:flex;align-items:center;justify-content:center;z-index:99999;font-family:Outfit,sans-serif';
        overlay.innerHTML = `
            <div style="background:#0d0e12;border:1px solid #1a1b1f;border-radius:12px;padding:40px;width:320px;text-align:center">
                <h2 style="color:#e2e4e9;margin:0 0 24px;font-size:20px">🍸 Liquor License Tracker</h2>
                <input id="auth-user" placeholder="Username" style="width:100%;padding:10px 14px;margin-bottom:12px;background:#16171c;border:1px solid #2a2b30;border-radius:8px;color:#e2e4e9;font-size:14px;box-sizing:border-box">
                <input id="auth-pass" type="password" placeholder="Password" style="width:100%;padding:10px 14px;margin-bottom:16px;background:#16171c;border:1px solid #2a2b30;border-radius:8px;color:#e2e4e9;font-size:14px;box-sizing:border-box">
                <button id="auth-btn" style="width:100%;padding:10px;background:#3b82f6;color:#fff;border:none;border-radius:8px;font-size:14px;cursor:pointer;font-family:Outfit,sans-serif">Sign In</button>
                <p id="auth-err" style="color:#ef4444;margin:12px 0 0;font-size:13px;display:none">Invalid credentials</p>
            </div>`;
        document.body.appendChild(overlay);

        const btn = document.getElementById('auth-btn');
        const userInput = document.getElementById('auth-user');
        const passInput = document.getElementById('auth-pass');
        const err = document.getElementById('auth-err');

        function attempt() {
            const u = userInput.value.toLowerCase().trim();
            const p = passInput.value;
            if (VALID_USERS[u] && VALID_USERS[u] === p) {
                localStorage.setItem(SESSION_KEY, JSON.stringify({user: u, ts: Date.now()}));
                location.reload();
            } else {
                err.style.display = 'block';
            }
        }

        btn.addEventListener('click', attempt);
        passInput.addEventListener('keydown', e => { if (e.key === 'Enter') attempt(); });
    }

    if (!checkSession()) {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', showLogin);
        } else {
            showLogin();
        }
    }
})();
