/* page-embed.jsx — the wrapper index.html mounts. No part of the animation
   lives here: it chooses a palette, hands off to LoomSurveyEmbed from the
   vendored loom-survey-scene.jsx, and does two things the design's own host
   has no reason to.

   It follows the reader's colour scheme, live. Both palettes ship in the
   scene file and they are this page's own tokens, so the canvas matches the
   section it sits in instead of being a screen bolted onto it.

   It turns the captions off with an actual boolean. LoomSurveyEmbed tests
   `props.captions !== false`, and an attribute on the <x-import> tag can only
   ever be a string — `captions="false"` is the string "false", which is not
   `false`, so the captions would stay on. This is the only way to pass one. */

function LoomSurveyPageEmbed() {
  const Embed = window.LoomSurveyEmbed;

  const mq = React.useMemo(
    () => (typeof window.matchMedia === 'function'
      ? window.matchMedia('(prefers-color-scheme: dark)')
      : null),
    [],
  );
  const [dark, setDark] = React.useState(() => !!(mq && mq.matches));

  React.useEffect(() => {
    if (!mq) return;
    const onChange = (e) => setDark(e.matches);
    // Safari below 14 only has the deprecated listener pair.
    if (mq.addEventListener) {
      mq.addEventListener('change', onChange);
      return () => mq.removeEventListener('change', onChange);
    }
    mq.addListener(onChange);
    return () => mq.removeListener(onChange);
  }, [mq]);

  // ?embed marks the copy index.html frames, where the transport is hidden
  // (see the host page's <style>). Set from here rather than from a script in
  // the head because the transport only exists once this has mounted anyway.
  React.useEffect(() => {
    if (!new URLSearchParams(window.location.search).has('embed')) return;
    document.documentElement.setAttribute('data-embed', '');
    return () => document.documentElement.removeAttribute('data-embed');
  }, []);

  return <Embed theme={dark ? 'dark' : 'light'} captions={false} />;
}

window.LoomSurveyPageEmbed = LoomSurveyPageEmbed;
