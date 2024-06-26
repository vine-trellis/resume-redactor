from resume.domain import model, redaction


def main():
    with open("./resume.pdf", "rb") as f:
        dirty_bytes =f.read()
    redacted_bytes = model.redact_pdf(
                bytes=dirty_bytes,
                redaction_strategies=[
                    redaction.Top30Percent(),
                    redaction.Bottom10Percent(),
                    redaction.ImageRedactor(),
                    redaction.LinkRedactor(),
                    redaction.MetadataRedactor(),
                ],
            )
    with open("./redacted.pdf", "wb") as f:
        f.write(redacted_bytes)

if __name__ == "__main__":
    main()
