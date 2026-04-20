require 'jsonpath'



data = [
  { "title" => "blablabl" }
]

pp JsonPath.on(data, "$[?(@.dcTitleLangAware == nil)]['title']")


❌ What you MUST NOT use (even if it looks correct)
These will silently fail:
Ruby$[?(!@.dcTitleLangAware)]@.[?(!@.dcTitleLangAware)]@.[?(@.dcTitleLangAware == null)]@.[?(!@.dcTitleLangAware)].titleMeer regels weergeven
Why:

The Ruby gem does not treat missing keys as boolean‑true under negation
The gem swallows the condition entirely
