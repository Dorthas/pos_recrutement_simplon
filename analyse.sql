-- Requête pour obtenir le chiffre d'affaires total 
SELECT ROUND(SUM(v.Quantite * p.Prix),2) as Chiffre_Affaires_Total
FROM ventes v
JOIN produits p ON v.ID_Reference_Produit = p.ID_Reference_Produit;

-- Requête pour obtenir les ventes par produit
SELECT 
    p.Nom, 
    p.ID_Reference_Produit,
    SUM(v.Quantite) as Quantite_Vendue, 
    ROUND(SUM(v.Quantite * p.Prix),2) as Chiffre_Affaires
FROM ventes v
JOIN produits p ON v.ID_Reference_Produit = p.ID_Reference_Produit
GROUP BY p.Nom, p.ID_Reference_Produit
ORDER BY Chiffre_Affaires DESC;

-- Requête pour obtenir les ventes par région (ville)
SELECT 
    m.Ville, 
    SUM(v.Quantite) as Quantite_Vendue, 
    ROUND(SUM(v.Quantite * p.Prix),2) as Chiffre_Affaires
FROM ventes v
JOIN produits p ON v.ID_Reference_Produit = p.ID_Reference_Produit
JOIN magasins m ON v.ID_Magasin = m.ID_Magasin
GROUP BY m.Ville
ORDER BY Chiffre_Affaires DESC;