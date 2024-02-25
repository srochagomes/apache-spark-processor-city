package br.com.search.cities.functions;

import org.apache.commons.codec.language.bm.NameType;
import org.apache.commons.codec.language.bm.PhoneticEngine;
import org.apache.commons.codec.language.bm.RuleType;

public class PhoneticUDF {

    // Método para calcular o código fonético de uma string
    public static String phoneticCode(String str) {
        // Criar uma instância de PhoneticEngine
        PhoneticEngine engine = new PhoneticEngine(NameType.GENERIC, RuleType.APPROX,true);
        // Calcular o código fonético da string de entrada
        return engine.encode(str.toLowerCase());
    }
}