javascript:(function () {
    /* 1. Достаем запрос */
    let queryInput = document.querySelector('input[name="text"]');
    let rawTitle = queryInput ? queryInput.value.trim() : "yandex_answer";
    let fileName = rawTitle.replace(/[\\\/:\*\?"<>\|]/g, '_').substring(0, 50);

    let text = rawTitle + "\n";
    text += "=== ===\n\n";

    /* 2. Основной текст ответа */
    document.querySelectorAll('h2.FuturisTitle, .FuturisMarkdown-Paragraph, .FuturisMarkdown-ListItem').forEach(el => {
        if (el.tagName === 'H2') text += "\n" + el.innerText.toUpperCase() + "\n"; else if (el.tagName === 'LI') text += "• " + el.innerText + "\n"; else {
            let content = el.innerText.trim();
            if (content) text += content + "\n";
        }
    });
    text += "\n=== ===\n";

    /* 3. Сбор ссылок-источников */
    let sourceLinks = document.querySelectorAll('a.FuturisSource');
    if (sourceLinks.length > 0) {
        sourceLinks.forEach((a, index) => {
            text += `[${index + 1}] ${a.href}\n`;
        });
    } else {
        text += "No data\n";
    }

    /* 4. Скачивание */
    let blob = new Blob([text], {type: "text/plain;charset=utf-8"});
    let a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = fileName + ".txt";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
})();