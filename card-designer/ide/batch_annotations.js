// Browser evaluation function. Coordinates are in native card pixels.
async root => {
    await document.fonts.ready;
    const images = [...root.querySelectorAll('img[src]')];
    const decoded = await Promise.allSettled(images.map(image => image.decode()));
    const origin = root.getBoundingClientRect();
    const box = rect => [rect.left - origin.left, rect.top - origin.top,
        rect.right - origin.left, rect.bottom - origin.top];
    const visible = element => element.checkVisibility({visibilityProperty: true, opacityProperty: true});
    const elements = [...root.querySelectorAll('[data-class]')].filter(visible)
        .map(element => ({class: element.dataset.class, bbox: box(element.getBoundingClientRect())}));
    const words = [];
    const fonts = new Set();
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    while (walker.nextNode()) {
        const node = walker.currentNode;
        const element = node.parentElement;
        if (element.closest('style, script, template') || !visible(element)) continue;
        const field = element.closest('[data-field]')?.dataset.field;
        const labelSelector = '.field-label, label';
        let label = element.closest(labelSelector);
        for (let parent = element; !label && parent !== root; parent = parent.parentElement) {
            label = [...parent.children].find(child => child.matches(labelSelector));
        }
        const style = getComputedStyle(element);
        const clip = [0, 0, origin.width, origin.height];
        for (let parent = element; parent !== root; parent = parent.parentElement) {
            const overflow = getComputedStyle(parent);
            if (overflow.overflowX === 'visible' && overflow.overflowY === 'visible') continue;
            const [left, top, right, bottom] = box(parent.getBoundingClientRect());
            if (overflow.overflowX !== 'visible') {
                clip[0] = Math.max(clip[0], left);
                clip[2] = Math.min(clip[2], right);
            }
            if (overflow.overflowY !== 'visible') {
                clip[1] = Math.max(clip[1], top);
                clip[3] = Math.min(clip[3], bottom);
            }
        }
        for (const match of node.data.matchAll(/\S+/gu)) {
            const range = document.createRange();
            range.setStart(node, match.index);
            range.setEnd(node, match.index + match[0].length);
            const boxes = [...range.getClientRects()].filter(rect => rect.width && rect.height).map(box);
            if (!boxes.length) continue;
            fonts.add(style.fontFamily);
            const clipped = boxes.some(([left, top, right, bottom]) =>
                left < clip[0] - 1 || top < clip[1] - 1 || right > clip[2] + 1 || bottom > clip[3] + 1);
            words.push({text: match[0], field: field ?? label?.innerText ?? null,
                role: element.closest(labelSelector) ? 'label' : 'value', boxes, clipped});
        }
    }
    return {elements, words, fonts: [...fonts].sort(),
        broken_images: decoded.flatMap((result, index) =>
            result.status === 'rejected' ? [images[index].getAttribute('src').slice(0, 160)] : [])};
}
