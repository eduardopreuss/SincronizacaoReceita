package com.ibm.sincronizacaoreceita.validation;

import com.ibm.sincronizacaoreceita.domain.Account;
import java.util.List;

public class Validator {

    // I could have throw an exception here but since this application outputs a column called "resultado"
    // I guess it's not what this IBM wants for this application

    // Most of this logic already exists on ReceitaService but a real external service might take some time send a response so I've made my own validation
    // If any data fails on my validation, ReceitaService will not be called increasing this app performance

    // On ReceitaService we have (same for accountNumber):
    // Formato conta: 0000 // I guess it says "only numbers of 4 digits are allowed"
    // if (agencia == null || agencia.length() != 4) { // I guess it says "only strings with length of 4 are allowed"
    // So for now I'll validate according to "Formato conta: 000000" but I need might change it in the future

    // I should add more validations like check the strings in the first line (agencia;conta;saldo;status)
    // Instead of just skipping it but for now I'll keep it simple

    //TODO: implement constant for the errors messages
    //TODO: implement more sophisticated ways to do a validation using ValidatingItemProcessor or external libraries
    public Account validateAccount(Account account) {

        final String fourDigits = "[0-9]{4}+"; // 0000
        final String accountNumberPattern = "[0-9]{5}+[-]+[0-9]{1}+"; // 00000-0
        final String balancePatter = "[-]?+[0-9]+[,]+[0-9]+"; // 00000000000,0000000000

        if (!account.getAgency().matches(fourDigits))
            account.setResult("Erro: agencia invalida"); // since input CSV file is in portuguese I'll return results in the same language

        if (!account.getAccountNumber().matches(accountNumberPattern))
            account.setResult("Erro: conta invalida");

        if (!List.of("A","I","B","P").contains(account.getStatus()))
            account.setResult("Erro: status invalido");


        if(!account.getBalance().matches(balancePatter))
            account.setResult("Erro: saldo invalido");

        return account;

    }
}
