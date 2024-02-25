package br.com.search.cities;

import br.com.search.cities.data.City;
import org.apache.commons.codec.language.Soundex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;

//@SpringBootTest
@ExtendWith(MockitoExtension.class)
class CitiesApplicationTests {

	@Test
	void contextLoads() {

		// Casos de teste
		String[] testCases = {"Hello", "World", "OpenAI", "GPT", "Test"};

		// Chamar a função soundexFunction e comparar os resultados
		for (String testCase : testCases) {
			String expectedResult = getExpectedSoundex(testCase); // Obter o Soundex esperado
			String result = City.soundexFunction(testCase); // Chamar a função soundexFunction
			System.out.println("Input: " + testCase + ", Expected Soundex: " + expectedResult + ", Result: " + result);
			if (expectedResult.equals(result)) {
				System.out.println("Test passed!");
			} else {
				System.out.println("Test failed!");
			}
		}
	}
	private static String getExpectedSoundex(String str) {
		Soundex soundex = new Soundex();
		return soundex.soundex(str);
	}
}
