package org.solbase;

import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.EmbeddedSortField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.MissingStringLastComparatorSource;

public class SolbaseStringField extends StrField {

	protected SortField getStringSort(SchemaField field, boolean reverse) {
		return getStringSortField(field.getName(), reverse, field.sortMissingLast(), field.sortMissingFirst());
	}

	private SortField getStringSortField(String fieldName, boolean reverse, boolean nullLast, boolean nullFirst) {
		if (nullLast) {
			if (!reverse)
				return new EmbeddedSortField(fieldName, nullStringLastComparatorSource);
			else
				return new EmbeddedSortField(fieldName, SortField.STRING, true);
		} else if (nullFirst) {
			if (reverse)
				return new EmbeddedSortField(fieldName, nullStringLastComparatorSource, true);
			else
				return new EmbeddedSortField(fieldName, SortField.STRING, false);
		} else {
			return new EmbeddedSortField(fieldName, SortField.STRING, reverse);
		}
	}
	
	static final FieldComparatorSource nullStringLastComparatorSource = new MissingStringLastComparatorSource(null);
}
