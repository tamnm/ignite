package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexableField;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;

class GridLuceneFieldFactory {

    private LuceneFieldFactory[] InternalFactories = new LuceneFieldFactory[]{
        ArrayOfStringFactory.Instance,
        ArrayOfNumberFactory.Instance,
            LongFieldFactory.Instance,
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

    static final GridLuceneFieldFactory Instance = new GridLuceneFieldFactory();
}

class ArrayOfStringFactory implements LuceneFieldFactory{

    @Override
    public boolean validateField(Object fieldValue) {
        Class cls = fieldValue.getClass();
        if(!cls.isArray()) return false;

        int size = Array.getLength(fieldValue);
        if(size ==0) return false;

        Object first = Array.get(fieldValue,0);
        return first instanceof String;
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

class ArrayOfNumberFactory implements LuceneFieldFactory{

    @Override
    public boolean validateField(Object fieldValue) {
        Class cls = fieldValue.getClass();
        if(!cls.isArray()) return false;

        int size = Array.getLength(fieldValue);
        if(size ==0) return false;

        Object first = Array.get(fieldValue,0);
        return first instanceof Number;
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
    public Collection<IndexableField> createFields(String name, Object value) {
        return new ArrayList<IndexableField>() {
            {
                add(PrimitiveFieldFactory.Instance.createNumberField(name, (Number) value));
            }
        };
    }

    static final LuceneFieldFactory Instance = new LongFieldFactory();
}

class PrimitiveFieldFactory{
    IndexableField createTextField(String name, String value){
        return new TextField(name, value, Field.Store.NO);
    }
    IndexableField createStringField(String name, String value){
        return new StringField(name, value, Field.Store.NO);
    }
    IndexableField createNumberField(String name, Number number){ return new LongPoint(name, number.longValue()); }

    static final PrimitiveFieldFactory Instance = new PrimitiveFieldFactory();
}

class DefaultFieldFactory implements LuceneFieldFactory{

    @Override
    public boolean validateField(Object fieldValue) {
        return true;
    }

    @Override
    public Collection<IndexableField> createFields(String name, Object value) {

        return new ArrayList<IndexableField>() {
            {
                add(PrimitiveFieldFactory.Instance.createTextField(name, value.toString()));
            }
        };
    }

    static final LuceneFieldFactory Instance = new DefaultFieldFactory();
}

interface LuceneFieldFactory{
    boolean validateField(Object fieldValue);
    Collection<IndexableField> createFields(String name, Object value);
}
