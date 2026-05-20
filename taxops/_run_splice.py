"""One-off splice; safe to delete after run."""
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parent
    path = root / "ai_routes.py"
    frag = (root / "__new_ai_chat_submit__.txt").read_text(encoding="utf-8").rstrip() + "\n"
    text = path.read_text(encoding="utf-8")
    anchor = '\n\n@ai.post("/return/<int:return_id>/draft-email")'
    start = text.index("def _ai_chat_submit():")
    end = text.index(anchor, start)
    new_text = text[:start] + frag + text[end:]
    path.write_text(new_text, encoding="utf-8")
    old_len = end - start
    print(f"replaced chars {old_len} -> {len(frag)}")


if __name__ == "__main__":
    main()
