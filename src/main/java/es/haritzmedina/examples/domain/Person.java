package es.haritzmedina.examples.domain;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by Haritz Medina on 13/10/2014.
 */
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Data public class Person implements Serializable {
    private String firstname;
    private String surname;
}
