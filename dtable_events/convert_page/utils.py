async def wait_for_images(page):
    await page.evaluate("""() => {
        window.scrollTo(0, document.body.scrollHeight);
    }""")
    await page.evaluate("""
        () => {
            const images = Array.from(document.images);
            return Promise.all(
                images.map(img => {
                    if (img.complete) return Promise.resolve();
                    return new Promise(resolve => {
                        img.onload = img.onerror = resolve;
                    });
                })
            );
        }
    """)
