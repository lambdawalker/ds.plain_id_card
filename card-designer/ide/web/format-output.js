export async function formatOutput(source, parser) {
  try {
    const [{ format }, plugin] = await Promise.all([
      import('prettier/standalone'),
      parser === 'html' ? import('prettier/plugins/html') : import('prettier/plugins/postcss'),
    ]);
    return await format(source, { parser, plugins: [plugin], embeddedLanguageFormatting: 'off' });
  } catch {
    return source;
  }
}
