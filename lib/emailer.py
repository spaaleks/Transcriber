import re, smtplib, ssl, time, base64, html as _html
from email.message import EmailMessage
from pathlib import Path
from typing import Optional
from bs4 import BeautifulSoup

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def load_recipients(path: Path) -> list[str]:
    if not path.exists():
        return []
    recs: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        addr = line.strip()
        if not addr or addr.startswith("#"):
            continue
        if EMAIL_RE.match(addr):
            recs.append(addr)
    return recs

def load_group_recipients(settings, group: str) -> list[str]:
    return load_recipients(settings.recipients_dir / f"recipients_{group}.txt")

def unique_preserve_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def _ssl_context(ca_file: str | None, verify: bool) -> ssl.SSLContext:
    if verify:
        return ssl.create_default_context(cafile=ca_file) if ca_file else ssl.create_default_context()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

def _html_to_text(s: str) -> str:
    s = re.sub(r"<(br|BR)\s*/?>", "\n", s)
    s = re.sub(r"</p\s*>", "\n\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    return _html.unescape(s).strip()

def process_inline_images(html: str) -> tuple[str, dict[str, Path]]:
    """Replace <img src="static/..."> with cid: and collect files to embed from project root."""
    project_root = Path(__file__).resolve().parent.parent  # assumes lib/ is a package dir
    soup = BeautifulSoup(html, "html.parser")
    inline_images: dict[str, Path] = {}
    counter = 0
    for img in soup.find_all("img", src=True):
        src = img["src"]
        if src.startswith("static/"):
            fp = project_root / src
            if fp.exists():
                cid = f"img{counter}"
                counter += 1
                img["src"] = f"cid:{cid}"
                inline_images[cid] = fp
    return str(soup), inline_images

def _subst_vars(s: str, variables: Optional[dict[str, str]] = None) -> str:
    if not variables:
        return s
    for k, v in variables.items():
        s = s.replace("{" + k + "}", str(v))
    return s

def _render_html_template(settings,
                          variables: Optional[dict[str, str]] = None,
                          override_body_html: Optional[str] = None) -> str:
    """
    - If neither override nor MAIL_BODY_HTML(_FILE)/MAIL_BODY_HTML is set -> templates/mail/default.html
    - Otherwise wrap chosen body inside templates/mail/custom.html at {{ content }}
    """
    project_root = Path(__file__).resolve().parent.parent
    template_dir = project_root / "templates" / "mail"

    body_src: Optional[str] = None
    if override_body_html and override_body_html.strip():
        body_src = override_body_html
    else:
        body_file = getattr(settings, "mail_body_html_file", None)
        body_inline = getattr(settings, "mail_body_html", None)
        if body_file:
            body_src = Path(body_file).read_text(encoding="utf-8")
        elif body_inline:
            body_src = body_inline

    if body_src:
        wrapper = (template_dir / "custom.html").read_text(encoding="utf-8")
        content = _subst_vars(body_src, variables)
        return wrapper.replace("{{ content }}", content)

    default_html = (template_dir / "default.html").read_text(encoding="utf-8")
    return _subst_vars(default_html, variables)

def send_email(settings, to_addr: str, subject: str, body_text: str,
               attachment_path: Optional[Path] = None,
               body_html: Optional[str] = None) -> None:
    msg = EmailMessage()
    sender = settings.smtp_from_header or settings.smtp_sender or ""
    msg["From"] = sender
    msg["To"] = to_addr
    msg["Subject"] = subject

    inline_images = {}
    if body_html:
        body_html, inline_images = process_inline_images(body_html)
        fallback = body_text or _html_to_text(body_html)
        msg.set_content(fallback)
        msg.add_alternative(body_html, subtype="html")
        if inline_images:
            html_part = msg.get_payload()[-1]
            for cid, img_path in inline_images.items():
                data = img_path.read_bytes()
                subtype = "svg+xml" if img_path.suffix.lower() == ".svg" else img_path.suffix.lower().lstrip(".")
                html_part.add_related(data, maintype="image", subtype=subtype, cid=f"<{cid}>")
    else:
        msg.set_content(body_text or "")

    if attachment_path:
        data = attachment_path.read_bytes()
        msg.add_attachment(data, maintype="text", subtype="plain", filename=attachment_path.name)

    user = (settings.smtp_user or "").strip()
    pwd  = (settings.smtp_pass or "").strip()

    if settings.smtp_use_ssl:
        context = _ssl_context(settings.smtp_ca_file, settings.smtp_verify)
        with smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port, context=context, timeout=30) as s:
            s.ehlo()
            if user or pwd:
                auth_caps = s.esmtp_features.get("auth", "").upper()
                if "PLAIN" in auth_caps:
                    authz = f"\0{user}\0{pwd}".encode("utf-8")
                    s.docmd("AUTH", "PLAIN " + base64.b64encode(authz).decode("ascii"))
                else:
                    s.login(user, pwd)
            s.send_message(msg)
    else:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=30) as s:
            s.ehlo()
            if settings.smtp_use_tls:
                s.starttls(context=_ssl_context(settings.smtp_ca_file, settings.smtp_verify))
                s.ehlo()
            if user or pwd:
                auth_caps = s.esmtp_features.get("auth", "").upper()
                if "PLAIN" in auth_caps:
                    authz = f"\0{user}\0{pwd}".encode("utf-8")
                    s.docmd("AUTH", "PLAIN " + base64.b64encode(authz).decode("ascii"))
                else:
                    s.login(user, pwd)
            s.send_message(msg)

def notify_recipients(settings, name: str, slug: str, txt_path: Path, log_cb, group: str = "none"):
    main_list = load_recipients(settings.recipients_file)
    group_list = load_group_recipients(settings, group)
    recs = unique_preserve_order([*group_list, *main_list])

    if not recs:
        log_cb(f"No recipients found for group '{group}' and main. Skipping email.")
        return
    if not (settings.smtp_host and settings.smtp_sender):
        log_cb("SMTP not configured (SMTP_HOST or SMTP_SENDER missing). Skipping email.")
        return

    sent = 0
    for addr in recs:
        try:
            subject = settings.mail_subject.format(name=name, slug=slug)
            body = settings.mail_body.format(name=name, slug=slug)
            variables = {"name": name, "slug": slug, "group": group}
            body_html = _render_html_template(settings, variables)

            send_email(
                settings=settings,
                to_addr=addr,
                subject=subject,
                body_text=body,
                attachment_path=txt_path,
                body_html=body_html,
            )
            sent += 1
            log_cb(f"Email sent to {addr}")
            time.sleep(0.3)
        except Exception as e:
            log_cb(f"Email FAILED to {addr}: {e}")
    log_cb(f"Email summary (group='{group}'): {sent}/{len(recs)} sent")

def first_recipient(path: Path) -> Optional[str]:
    recs = load_recipients(path)
    return recs[0] if recs else None

def smtp_smoke_test(settings) -> tuple[bool, str]:
    if not (settings.smtp_host and settings.smtp_sender):
        return False, "SMTP_HOST or SMTP_SENDER not configured"
    to_addr = first_recipient(settings.recipients_file)
    if not to_addr:
        return False, f"No recipients in {settings.recipients_file}"
    try:
        subject = f"[SMTP TEST] {settings.mail_subject.format(name='TEST', slug='test')}"
        body = "This is a SMTP test from the transcriber service.\nIf you received this, SMTP works."
        test_fragment = "<p>Hi,</p><p>This is a SMTP test from the transcriber service.<br/>If you received this, SMTP works.</p>"
        body_html = _render_html_template(settings, override_body_html=test_fragment)

        send_email(
            settings=settings,
            to_addr=to_addr,
            subject=subject,
            body_text=body,
            attachment_path=None,
            body_html=body_html,
        )
        return True, f"Sent test email to {to_addr}"
    except Exception as e:
        return False, f"SMTP test failed: {e}"
