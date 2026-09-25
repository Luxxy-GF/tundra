use anyhow::Context;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ed25519_dalek::{
    SigningKey,
    pkcs8::{DecodePrivateKey, EncodePrivateKey, spki::der::pem::LineEnding},
};
use rand::Rng as _;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CertificateSigningRequestParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
    SanType, SerialNumber, string::Ia5String,
};
use std::{fs, path::Path};
use time::{Duration, OffsetDateTime};
use tundra_common::{
    hash::{Hash32, sha256},
    jwt::JwtIssuer,
};

pub const NODE_CERT_VALIDITY_DAYS: i64 = 90;

#[inline]
pub fn node_dns_name(uuid: &uuid::Uuid) -> String {
    format!("n{}.nodes.calagopus.internal", uuid.simple())
}

pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut out = [0; N];
    rand::rng().fill_bytes(&mut out);
    out
}

fn load_or_create_ca(dir: &Path) -> Result<(String, KeyPair), anyhow::Error> {
    const CA_VALIDITY_DAYS: i64 = 3650;

    let key_path = dir.join("ca.key.pem");
    let cert_path = dir.join("ca.crt.pem");

    if key_path.exists() && cert_path.exists() {
        let key = KeyPair::from_pkcs8_pem_and_sign_algo(
            &fs::read_to_string(&key_path)?,
            &PKCS_ECDSA_P256_SHA256,
        )
        .context("failed to load the CA key")?;
        return Ok((fs::read_to_string(&cert_path)?, key));
    }

    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "tundra node CA");
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];

    let now = OffsetDateTime::now_utc();
    params.not_before = now - Duration::hours(1);
    params.not_after = now + Duration::days(CA_VALIDITY_DAYS);

    let cert = params.self_signed(&key)?;
    write_private(&key_path, key.serialize_pem().as_bytes())?;
    fs::write(&cert_path, cert.pem())?;

    Ok((cert.pem(), key))
}

fn load_or_create_jwt(dir: &Path) -> Result<SigningKey, anyhow::Error> {
    let path = dir.join("jwt.key.pem");
    if path.exists() {
        return SigningKey::from_pkcs8_pem(&fs::read_to_string(&path)?)
            .context("failed to load the JWT signing key");
    }

    let key = SigningKey::from_bytes(&random_bytes());
    write_private(&path, key.to_pkcs8_pem(LineEnding::LF)?.as_bytes())?;

    Ok(key)
}

fn load_or_create_https(dir: &Path, sans: &[String]) -> Result<(Vec<u8>, Vec<u8>), anyhow::Error> {
    const HTTPS_VALIDITY_DAYS: i64 = 3650;

    let key_path = dir.join("https.key.pem");
    let cert_path = dir.join("https.crt.pem");

    let (cert_pem, key) = if key_path.exists() && cert_path.exists() {
        let key = KeyPair::from_pkcs8_pem_and_sign_algo(
            &fs::read_to_string(&key_path)?,
            &PKCS_ECDSA_P256_SHA256,
        )
        .context("failed to load the HTTPS key")?;
        (fs::read_to_string(&cert_path)?, key)
    } else {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let mut params = CertificateParams::new(sans.to_vec())?;
        params
            .distinguished_name
            .push(DnType::CommonName, "tundra panel");

        let now = OffsetDateTime::now_utc();
        params.not_before = now - Duration::hours(1);
        params.not_after = now + Duration::days(HTTPS_VALIDITY_DAYS);
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

        let cert: Certificate = params.self_signed(&key)?;
        write_private(&key_path, key.serialize_pem().as_bytes())?;
        fs::write(&cert_path, cert.pem())?;

        (cert.pem(), key)
    };

    Ok((pem_to_der(&cert_pem)?, key.serialize_der()))
}

fn pem_to_der(pem: &str) -> Result<Vec<u8>, anyhow::Error> {
    let mut body = String::new();
    for line in pem.lines() {
        if line.starts_with("-----") {
            continue;
        }

        body.extend(line.chars().filter(|c| !c.is_whitespace()));
    }

    STANDARD
        .decode(body)
        .context("failed to decode the PEM body")
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<(), anyhow::Error> {
    use std::io::Write;

    // a key can outlive its certificate, and create_new below must not trip over it
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }

    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .context(format!("failed to create {}", path.display()))?
        .write_all(bytes)?;

    Ok(())
}

pub struct PanelKeys {
    pub ca_cert_pem: String,
    ca_key: KeyPair,
    pub jwt: JwtIssuer,
    jwt_public: Hash32,

    pub https_cert_der: Vec<u8>,
    pub https_key_der: Vec<u8>,
    pub https_fingerprint: Hash32,
}

impl PanelKeys {
    pub fn load_or_create(dir: &Path, https_sans: &[String]) -> Result<Self, anyhow::Error> {
        fs::create_dir_all(dir).context(format!("failed to create {}", dir.display()))?;

        let (ca_cert_pem, ca_key) = load_or_create_ca(dir)?;
        let signing_key = load_or_create_jwt(dir)?;
        let jwt_public = Hash32(signing_key.verifying_key().to_bytes());
        let jwt = JwtIssuer::from_pkcs8_der(
            signing_key
                .to_pkcs8_der()
                .context("failed to encode the JWT signing key")?
                .as_bytes(),
        );
        let (https_cert_der, https_key_der) = load_or_create_https(dir, https_sans)?;

        let https_fingerprint = sha256(&https_cert_der);
        fs::write(
            dir.join("https.fingerprint"),
            format!("{https_fingerprint}\n"),
        )?;

        Ok(Self {
            ca_cert_pem,
            ca_key,
            jwt,
            jwt_public,

            https_cert_der,
            https_key_der,
            https_fingerprint,
        })
    }

    #[inline]
    pub fn jwt_public(&self) -> Hash32 {
        self.jwt_public
    }

    // the subject name comes from the uuid the token authenticated, not from the CSR
    pub fn sign_node_csr(
        &self,
        csr_pem: &str,
        node: &uuid::Uuid,
    ) -> Result<SignedCert, anyhow::Error> {
        let mut csr = CertificateSigningRequestParams::from_pem(csr_pem)
            .context("failed to parse the certificate signing request")?;

        if csr.public_key.algorithm() != &PKCS_ECDSA_P256_SHA256 {
            return Err(anyhow::anyhow!("node keys must be ECDSA P-256"));
        }

        let dns = node_dns_name(node);
        let now = OffsetDateTime::now_utc();
        csr.params.subject_alt_names = vec![SanType::DnsName(Ia5String::try_from(dns.clone())?)];
        csr.params.distinguished_name = rcgen::DistinguishedName::new();
        csr.params.distinguished_name.push(DnType::CommonName, dns);
        csr.params.not_before = now - Duration::hours(1);
        csr.params.not_after = now + Duration::days(NODE_CERT_VALIDITY_DAYS);
        csr.params.is_ca = IsCa::ExplicitNoCa;
        csr.params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        csr.params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        csr.params.serial_number = Some(SerialNumber::from_slice(&random_bytes::<16>()));
        csr.params.use_authority_key_identifier_extension = true;

        let issuer = Issuer::from_ca_cert_pem(&self.ca_cert_pem, &self.ca_key)
            .context("failed to load the signing CA")?;
        let cert = csr.signed_by(&issuer)?;

        Ok(SignedCert {
            sha256: sha256(cert.der()),
            pem: cert.pem(),
        })
    }
}

pub struct SignedCert {
    pub pem: String,
    pub sha256: Hash32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tundra_common::jwt::{ConnectClaims, Expect, JwtClient};

    fn tempdir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("tundra-keys-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    // PanelKeys::load_or_create

    #[test]
    fn keys_are_generated_once_and_reloaded_verbatim() {
        let dir = tempdir("reload");
        let sans = vec!["panel.test".to_owned()];

        let a = PanelKeys::load_or_create(&dir, &sans).unwrap();
        let b = PanelKeys::load_or_create(&dir, &sans).unwrap();

        assert_eq!(a.ca_cert_pem, b.ca_cert_pem);
        assert_eq!(a.https_fingerprint, b.https_fingerprint);
        assert_eq!(
            fs::read_to_string(dir.join("https.fingerprint"))
                .unwrap()
                .trim(),
            a.https_fingerprint.to_hex()
        );
        assert_eq!(a.jwt_public(), b.jwt_public());
        assert_eq!(a.https_cert_der, b.https_cert_der);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_https_fingerprint_is_the_digest_a_client_would_pin() {
        let dir = tempdir("fingerprint");
        let keys = PanelKeys::load_or_create(&dir, &["panel.test".to_owned()]).unwrap();

        assert_eq!(keys.https_fingerprint, sha256(&keys.https_cert_der));
        // 0x30 is the DER SEQUENCE tag every X.509 certificate opens with
        assert_eq!(keys.https_cert_der[0], 0x30);
        fs::remove_dir_all(&dir).ok();
    }

    // PanelKeys::sign_node_csr

    #[test]
    fn a_signed_node_csr_carries_the_uuid_derived_name() {
        let dir = tempdir("csr");
        let keys = PanelKeys::load_or_create(&dir, &["panel.test".to_owned()]).unwrap();

        let node = uuid::Uuid::new_v4();
        let node_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "whatever the node claims");
        let csr = params.serialize_request(&node_key).unwrap();

        let signed = keys.sign_node_csr(&csr.pem().unwrap(), &node).unwrap();
        let der = pem_to_der(&signed.pem).unwrap();
        assert_eq!(signed.sha256, sha256(&der));

        let (_, parsed) = x509_parser::parse_x509_certificate(&der).unwrap();
        let san = parsed.subject_alternative_name().unwrap().unwrap();
        let names: Vec<_> = san
            .value
            .general_names
            .iter()
            .map(|g| format!("{g:?}"))
            .collect();
        assert!(
            names.iter().any(|n| n.contains(&node_dns_name(&node))),
            "{names:?}"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn sign_node_csr_rejects_an_unparseable_request() {
        let dir = tempdir("badcsr");
        let keys = PanelKeys::load_or_create(&dir, &["panel.test".to_owned()]).unwrap();

        assert!(
            keys.sign_node_csr("not a csr", &uuid::Uuid::new_v4())
                .is_err()
        );
        fs::remove_dir_all(&dir).ok();
    }

    // PanelKeys::jwt

    #[test]
    fn tokens_issued_by_the_panel_verify_against_the_published_key() {
        let dir = tempdir("jwt");
        let keys = PanelKeys::load_or_create(&dir, &["panel.test".to_owned()]).unwrap();

        let src = uuid::Uuid::new_v4();
        let dst = uuid::Uuid::new_v4();
        let cert = sha256(b"node cert der");
        let token = keys
            .jwt
            .create(&ConnectClaims::new(src, dst, &cert, 1000))
            .unwrap();

        let published = JwtClient::new(&keys.jwt_public()).unwrap();
        let expect = Expect {
            audience: dst,
            subject: src,
            client_cert: cert,
            leeway: 60,
        };
        assert!(published.validate_connect(&token, &expect, 1000).is_ok());
        fs::remove_dir_all(&dir).ok();
    }
}
