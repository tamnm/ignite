package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexableField;

import java.lang.reflect.Array;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;

class GridLuceneFieldFactory {

    public static final String NUMERIC_FIELD_STORE_POSTFIX = "_STORED";
    public static final String RAW_TEXT_FIELD_NAME_POSTFIX = "_RAW";

    private LuceneFieldFactory[] InternalFactories = new LuceneFieldFactory[]{
            ArrayOfStringFactory.Instance,
            ArrayOfNumberFactory.Instance,
            LongFieldFactory.Instance,
            BooleanFieldFactory.Instance,
            StringFactory.Instance,
            DefaultFieldFactory.Instance
    };

    Collection<IndexableField> createFields(String name, Object value) {
        for (LuceneFieldFactory internalFactory : InternalFactories) {
            if (internalFactory.validateField(value)) {
                return internalFactory.createFields(name, value);
            }
        }
        return null;
    }

    Collection<String> getGeneratedIndexableName(String name, Class cls) {
        for (LuceneFieldFactory internalFactory : InternalFactories) {
            if (internalFactory.validateField(cls)) {
                return internalFactory.getGeneratedIndexableName(name, cls);
            }
        }
        return null;
    }

    public static String NumericStoredName(String fieldName){
        return fieldName + GridLuceneFieldFactory.NUMERIC_FIELD_STORE_POSTFIX;
    }

    public static String RawTextFieldName(String fieldName){
        return fieldName+GridLuceneFieldFactory.RAW_TEXT_FIELD_NAME_POSTFIX;
    }

    static final GridLuceneFieldFactory Instance = new GridLuceneFieldFactory();

    public static class TextUtils{
        public static String normalize(String s){
            s = Normalizer.normalize(s, Normalizer.Form.NFD);
            s = s.replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
                 .replace('đ','d');
            return s;
        }
    }
}

class ArrayOfStringFactory implements LuceneFieldFactory{

    @Override
    public boolean validateField(Object fieldValue) {
        return validateField(fieldValue.getClass());
    }

    @Override
    public boolean validateField(Class cls) {
        return cls == String[].class;
    }

    @Override
    public Collection<String> getGeneratedIndexableName(String name, Class cls) {
        return new ArrayList<String>()
        {
            {
                add(name);
            }
        };
    }

    @Override
    public Collection<IndexableField> createFields(String name, Object value) {
        int length = Array.getLength(value);
        ArrayList<IndexableField> fields = new ArrayList<>();

        for(int i =0;i<length;i++){
            String s = (String) Array.get(value, i);
            fields.add(PrimitiveFieldFactory.Instance.createStringField(name, s));
        }
        return fields;
    }

    static final LuceneFieldFactory Instance = new ArrayOfStringFactory();
}

class StringFactory implements LuceneFieldFactory{

    @Override
    public boolean validateField(Object fieldValue) {
        return fieldValue instanceof String;
    }

    @Override
    public boolean validateField(Class cls) {
        return cls == String.class;
    }

    @Override
    public Collection<String> getGeneratedIndexableName(String name, Class cls) {
        return new ArrayList<String>() {
            {
                add(name);
                add(GridLuceneFieldFactory.RawTextFieldName(name));
            }
        };
    }

    @Override
    public Collection<IndexableField> createFields(String name, Object value) {
        final String rawFieldName = GridLuceneFieldFactory.RawTextFieldName(name);

        String s = GridLuceneFieldFactory.TextUtils.normalize(value.toString());

        Analyzer analyzer = new StandardAnalyzer();
        return new ArrayList<IndexableField>() {
            {
                add(PrimitiveFieldFactory.Instance.createTextField(name, s));
                add(PrimitiveFieldFactory.Instance.createTextField(rawFieldName, analyzer.tokenStream(rawFieldName, s)));
            }
        };
    }

    static final LuceneFieldFactory Instance = new StringFactory();
}

class ArrayOfNumberFactory implements LuceneFieldFactory{

    @Override
    public boolean validateField(Object fieldValue) {
        Class cls = fieldValue.getClass();
        return validateField(cls);
    }

    @Override
    public boolean validateField(Class cls) {
        return (cls == int[].class
                || cls== long[].class
                || cls == float[].class
                || cls==double[].class
                || cls== byte[].class
                || cls== short[].class
                || cls == Integer[].class
                || cls== Long[].class
                || cls == Float[].class
                || cls==Double[].class
                || cls== Byte[].class
                || cls== Short[].class
        );
    }

    @Override
    public Collection<String> getGeneratedIndexableName(String name, Class cls) {
        return new ArrayList<String>() {
            {
                add(name);
            }
        };
    }

    @Override
    public Collection<IndexableField> createFields(String name, Object value) {
        int length = Array.getLength(value);
        ArrayList<IndexableField> fields = new ArrayList<>();

        for(int i =0;i<length;i++){
            Number number = (Number) Array.get(value, i);
            fields.add(PrimitiveFieldFactory.Instance.createNumberField(name, number));
        }
        return fields;
    }

    static final LuceneFieldFactory Instance = new ArrayOfNumberFactory();
}

class LongFieldFactory implements LuceneFieldFactory{

    @Override
    public boolean validateField(Object fieldValue) {
        return fieldValue instanceof Number;
    }

    @Override
    public boolean validateField(Class cls) {
        return (cls == int.class
                || cls== long.class
                || cls == float.class
                || cls==double.class
                || cls== byte.class
                || cls== short.class
                || cls == Integer.class
                || cls== Long.class
                || cls == Float.class
                || cls==Double.class
                || cls== Byte.class
                || cls== Short.class
        );
    }

    @Override
    public Collection<String> getGeneratedIndexableName(String name, Class cls) {
        return new ArrayList<String>()
        {
            {
                add(name);
                add(GridLuceneFieldFactory.NumericStoredName(name));
            }
        };
    }

    @Override
    public Collection<IndexableField> createFields(String name, Object value) {
        return new ArrayList<IndexableField>() {
            {
                add(PrimitiveFieldFactory.Instance.createNumberField(name, (Number) value));
                add(PrimitiveFieldFactory.Instance.createStoredNumberField(GridLuceneFieldFactory.NumericStoredName(name), (Number) value));
            }
        };
    }

    static final LuceneFieldFactory Instance = new LongFieldFactory();
}

class BooleanFieldFactory implements LuceneFieldFactory{

    @Override
    public boolean validateField(Object fieldValue) {
        return validateField(fieldValue.getClass());
    }

    @Override
    public boolean validateField(Class cls) {
        return cls == boolean.class || cls == Boolean.class;
    }

    @Override
    public Collection<String> getGeneratedIndexableName(String name, Class cls) {
        return new ArrayList<String>()
        {
            {
                add(name);
            }
        };
    }

    @Override
    public Collection<IndexableField> createFields(String name, Object value) {
        return new ArrayList<IndexableField>() {
            {
                add(PrimitiveFieldFactory.Instance.createStringField(name, value.toString()));
            }
        };
    }

    static final LuceneFieldFactory Instance = new BooleanFieldFactory();
}

class PrimitiveFieldFactory{
    IndexableField createTextField(String name, String value){ return new TextField(name, value.toLowerCase(), Field.Store.YES); }
    IndexableField createTextField(String name, TokenStream stream){ return new TextField(name, stream); }
    IndexableField createStringField(String name, String value){ return new StringField(name, value.toLowerCase(), Field.Store.YES); }
    IndexableField createNumberField(String name, Number value){ return new LongPoint(name, value.longValue());  }
    IndexableField createStoredNumberField(String name, Number value){ return new StoredField(name, value.doubleValue()); }

    static final PrimitiveFieldFactory Instance = new PrimitiveFieldFactory();
}

class DefaultFieldFactory implements LuceneFieldFactory{

    @Override
    public boolean validateField(Object fieldValue) {
        return validateField(fieldValue.getClass());
    }

    @Override
    public boolean validateField(Class cls) {
        return true;
    }

    @Override
    public Collection<String> getGeneratedIndexableName(String name, Class cls) {
        return new ArrayList<String>()
        {
            {
                add(name);
            }
        };
    }

    @Override
    public Collection<IndexableField> createFields(String name, Object value) {

        return new ArrayList<IndexableField>() {
            {
                add(PrimitiveFieldFactory.Instance.createStringField(name, GridLuceneFieldFactory.TextUtils.normalize(value.toString())));
            }
        };
    }

    static final LuceneFieldFactory Instance = new DefaultFieldFactory();
}

interface LuceneFieldFactory{
    boolean validateField(Object fieldValue);
    boolean validateField(Class  cls);
    Collection<String> getGeneratedIndexableName(String name, Class cls);
    Collection<IndexableField> createFields(String name, Object value);
}
