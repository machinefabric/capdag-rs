//! Asking the fabric for a cap.
//!
//! A request is not a cap. A cap takes this and gives that; a request is a
//! QUESTION about caps, and may leave a side unasked: "what gives me this
//! file, whatever it takes?", "what can this input become?". Those are not cap
//! URNs with `media:` on a side — `media:` is the type "anything", a claim
//! about every input — they are queries with that side unknown.
//!
//! [`CapQuery`] is such a question, and [`MatchGrade`] is how a registered cap
//! answers it. A cap URN read as a request or as a pattern is one
//! ([`CapQuery::from_request`], [`CapQuery::from_pattern`]) — which is what
//! [`CapUrn::is_dispatchable`] and [`CapUrn::accepts`] ask — and so are the
//! questions no cap URN can spell ([`CapQuery::producing`],
//! [`CapQuery::consuming`], [`CapQuery::between`]).
//!
//! Every answer is decided by the proved model (`../formal/CapDAG/Query.lean`).

use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;
use std::collections::BTreeMap;
use std::fmt;
use tagged_urn::TaggedUrn;

/// How a registered cap answers a [`CapQuery`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchGrade {
    /// Guaranteed, and exactly what was asked on every side that was asked.
    Exact,
    /// Guaranteed: whatever the cap takes and gives, it is what was asked.
    Guaranteed,
    /// Not guaranteed and not excluded: it could be, and only running it
    /// tells. For exploring; a call is never routed on it.
    Possible,
    /// Excluded.
    None,
}

impl MatchGrade {
    /// Whether routing may act on it.
    pub fn is_guaranteed(self) -> bool {
        matches!(self, MatchGrade::Exact | MatchGrade::Guaranteed)
    }

    /// Whether a search should show it.
    pub fn is_possible(self) -> bool {
        self != MatchGrade::None
    }
}

/// A question about caps.
#[derive(Clone)]
pub struct CapQuery {
    formal: crate::formal::exec::WfQuery,
    asked: String,
}

impl fmt::Debug for CapQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CapQuery").field(&self.asked).finish()
    }
}

impl fmt::Display for CapQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.asked)
    }
}

/// The cap-tag pattern a query asks for: `cap:` with these tags. An empty map
/// asks for none.
fn tag_pattern(tags: &BTreeMap<String, String>) -> TaggedUrn {
    TaggedUrn::new(CapUrn::PREFIX.to_string(), tags.clone())
}

impl CapQuery {
    /// The cap URN `request`, read as a request: an input it leaves open is
    /// not established. What [`CapUrn::is_dispatchable`] asks.
    pub fn from_request(request: &CapUrn) -> Self {
        Self {
            formal: crate::formal::exec::query_of_request(request.formal().clone()),
            asked: format!("request {request}"),
        }
    }

    /// The cap URN `pattern`, read as a pattern over caps: an output it leaves
    /// open is not established. What [`CapUrn::accepts`] asks.
    pub fn from_pattern(pattern: &CapUrn) -> Self {
        Self {
            formal: crate::formal::exec::query_of_pattern(pattern.formal().clone()),
            asked: format!("pattern {pattern}"),
        }
    }

    /// Caps that GIVE `output`, whatever they take, with the cap-tags `tags`
    /// asks for (none, if empty).
    pub fn producing(output: &MediaUrn, tags: &BTreeMap<String, String>) -> Self {
        Self {
            formal: crate::formal::exec::query_producing(
                output.inner().formal().clone(),
                tag_pattern(tags).formal().clone(),
            ),
            asked: format!("anything giving {output}"),
        }
    }

    /// Caps that TAKE `input`, whatever they give: what this input can become
    /// in one step.
    pub fn consuming(input: &MediaUrn, tags: &BTreeMap<String, String>) -> Self {
        Self {
            formal: crate::formal::exec::query_consuming(
                input.inner().formal().clone(),
                tag_pattern(tags).formal().clone(),
            ),
            asked: format!("anything taking {input}"),
        }
    }

    /// Caps that take `input` and give `output`: nothing unknown.
    pub fn between(input: &MediaUrn, output: &MediaUrn, tags: &BTreeMap<String, String>) -> Self {
        Self {
            formal: crate::formal::exec::query_between(
                input.inner().formal().clone(),
                output.inner().formal().clone(),
                tag_pattern(tags).formal().clone(),
            ),
            asked: format!("anything taking {input} and giving {output}"),
        }
    }

    /// Whether `cap` is guaranteed to be what is asked.
    pub fn admits(&self, cap: &CapUrn) -> bool {
        crate::formal::exec::query_admits(self.formal.clone(), cap.formal().clone())
    }

    /// Whether `cap` could be what is asked.
    pub fn may_admit(&self, cap: &CapUrn) -> bool {
        crate::formal::exec::query_may_admit(self.formal.clone(), cap.formal().clone())
    }

    /// How `cap` answers.
    pub fn grade(&self, cap: &CapUrn) -> MatchGrade {
        match crate::formal::exec::query_grade(self.formal.clone(), cap.formal().clone()) {
            crate::formal::Grade::Exact => MatchGrade::Exact,
            crate::formal::Grade::Guaranteed => MatchGrade::Guaranteed,
            crate::formal::Grade::Possible => MatchGrade::Possible,
            crate::formal::Grade::None => MatchGrade::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap(s: &str) -> CapUrn {
        CapUrn::from_string(s).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    fn media(s: &str) -> MediaUrn {
        MediaUrn::from_string(s).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    fn no_tags() -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    fn pages() -> CapUrn {
        cap(r#"cap:disbind;in="media:ext=pdf";out="media:enc=utf-8;ext=txt;page""#)
    }

    fn to_jpeg() -> CapUrn {
        cap(r#"cap:convert-image;in="media:ext=png;image";out="media:ext=jpeg;image""#)
    }

    fn some_image() -> CapUrn {
        cap(r#"cap:render;in="media:ext=pdf";out="media:image""#)
    }

    fn passes_through() -> CapUrn {
        cap("cap:decimate-sequence;effect=none")
    }

    /// TEST12368: "what gives me this, whatever it takes?" and "what can this
    /// become?" are questions with a side unknown, and each cap answers with
    /// a grade.
    ///
    /// No cap URN can ask them: `media:` on a side is the type "anything", so
    /// a request spelled that way asked for a cap that takes everything, and
    /// found none. The unknown side asks nothing; the stated side is held to.
    #[test]
    fn test12368_a_question_may_leave_a_side_unknown() {
        let wants_jpeg = CapQuery::producing(&media("media:ext=jpeg;image"), &no_tags());
        assert_eq!(wants_jpeg.grade(&to_jpeg()), MatchGrade::Exact);
        assert!(wants_jpeg.admits(&to_jpeg()), "whatever it takes: a png here");
        // "Some image" is not a jpeg, and not excluded: possible, never routed on.
        assert_eq!(wants_jpeg.grade(&some_image()), MatchGrade::Possible);
        assert!(!wants_jpeg.admits(&some_image()) && wants_jpeg.may_admit(&some_image()));
        assert_eq!(wants_jpeg.grade(&pages()), MatchGrade::None);

        // Asked for any image, a jpeg is guaranteed to be one, not exactly it.
        let wants_image = CapQuery::producing(&media("media:image"), &no_tags());
        assert_eq!(wants_image.grade(&to_jpeg()), MatchGrade::Guaranteed);
        assert_eq!(wants_image.grade(&some_image()), MatchGrade::Exact);
        // A cap that gives `media:` promises no image at all.
        assert!(!wants_image.admits(&passes_through()));

        // What can a pdf become? Whatever takes a pdf — or takes anything.
        let has_pdf = CapQuery::consuming(&media("media:ext=pdf"), &no_tags());
        assert!(has_pdf.admits(&pages()) && has_pdf.admits(&some_image()));
        assert!(has_pdf.admits(&passes_through()), "it takes anything, a pdf included");
        assert!(!has_pdf.admits(&to_jpeg()), "a png converter does not take a pdf");
        assert_eq!(has_pdf.grade(&to_jpeg()), MatchGrade::None);

        // Both sides stated is the typed call.
        let pdf_to_image =
            CapQuery::between(&media("media:ext=pdf"), &media("media:image"), &no_tags());
        assert!(pdf_to_image.admits(&some_image()));
        assert!(!pdf_to_image.admits(&pages()) && !pdf_to_image.admits(&to_jpeg()));
    }

    /// TEST12369: the cap-tags a query asks for are matched against the tags
    /// the cap HAS — a cap's own list is complete.
    ///
    /// So asking that a tag be absent selects the caps that do not carry it,
    /// which no cap needs to declare; and a cap may carry tags nobody asked
    /// about.
    #[test]
    fn test12369_a_query_s_tags_are_asked_of_the_tags_a_cap_has() {
        let tagged = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
        };
        let any_image = media("media:image");

        let converters = CapQuery::producing(&any_image, &tagged(&[("convert-image", "*")]));
        assert!(converters.admits(&to_jpeg()));
        assert!(!converters.admits(&some_image()), "it renders; it is not tagged convert-image");

        let not_converters = CapQuery::producing(&any_image, &tagged(&[("convert-image", "!")]));
        assert!(not_converters.admits(&some_image()), "it does not have the tag");
        assert!(!not_converters.admits(&to_jpeg()), "it has it");
        assert_eq!(not_converters.grade(&to_jpeg()), MatchGrade::None);

        // The same holds of a request: `!x` is served by a cap silent on x.
        let request = cap(r#"cap:!convert-image;out="media:image""#);
        assert!(some_image().is_dispatchable(&request));
        assert!(!to_jpeg().is_dispatchable(&request));
        assert_eq!(CapQuery::from_request(&request).grade(&some_image()), MatchGrade::Exact);
    }
}
