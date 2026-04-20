using System.Security.Cryptography;
using System.Text;

namespace IT4You.Application.Helpers;

public static class EncryptionHelper
{
    // A chave secreta mencionada pelo usuário. Em um cenário real, 
    // isso deveria vir de uma variável de ambiente ou Secret Manager.
    private static readonly string SecretKey = "IT4You_SuperSecret_Key_2026!"; 

    public static string Encrypt(string plainText)
    {
        if (string.IsNullOrEmpty(plainText)) return plainText;

        using Aes aes = Aes.Create();
        // Usamos SHA256 para garantir que a chave tenha o tamanho correto para o AES-256
        byte[] key = SHA256.HashData(Encoding.UTF8.GetBytes(SecretKey));
        aes.Key = key;
        aes.GenerateIV();
        byte[] iv = aes.IV;

        using var encryptor = aes.CreateEncryptor(aes.Key, aes.IV);
        using var ms = new MemoryStream();
        // Grava o IV no início do stream para uso na descriptografia
        ms.Write(iv, 0, iv.Length);

        using (var cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write))
        using (var sw = new StreamWriter(cs))
        {
            sw.Write(plainText);
        }

        return Convert.ToBase64String(ms.ToArray());
    }

    public static string Decrypt(string cipherText)
    {
        if (string.IsNullOrEmpty(cipherText)) return cipherText;

        try
        {
            byte[] fullCipher = Convert.FromBase64String(cipherText);
            using Aes aes = Aes.Create();
            byte[] key = SHA256.HashData(Encoding.UTF8.GetBytes(SecretKey));
            aes.Key = key;

            byte[] iv = new byte[aes.BlockSize / 8];
            Array.Copy(fullCipher, 0, iv, 0, iv.Length);
            aes.IV = iv;

            using var decryptor = aes.CreateDecryptor(aes.Key, aes.IV);
            using var ms = new MemoryStream(fullCipher, iv.Length, fullCipher.Length - iv.Length);
            using var cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read);
            using var sr = new StreamReader(cs);

            return sr.ReadToEnd();
        }
        catch
        {
            // Se falhar a descriptografia, retorna o texto original 
            // (pode ser um valor legado não criptografado)
            return cipherText; 
        }
    }
}
