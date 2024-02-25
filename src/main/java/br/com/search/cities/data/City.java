package br.com.search.cities.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.codec.language.Soundex;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class City implements Serializable {
    private Integer linha;
    private String municipios;
    private String tipo;
    private String h;
    private String cep;
    private String paisSKU;
    private String municipioSKU;

    public static String soundexFunction(String str) {
        if (str != null) {
            Soundex soundex = new Soundex();
            return soundex.soundex(str);
        }
        return null;
    }
}
