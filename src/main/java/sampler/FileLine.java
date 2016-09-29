package sampler;

/** Wrapper class for printing as a csv line. */
final class FileLine {
    private final String languageCode;
    private final String wikiItem;
    private final Integer views;

    public FileLine(String languageCode, String wikiItem, Integer views) {
        this.languageCode = languageCode;
        this.wikiItem = wikiItem;
        this.views = views;
    }

    @Override
    public String toString() {
        return languageCode + "\t" + wikiItem + "\t" + views;
    }
}
