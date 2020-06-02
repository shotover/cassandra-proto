extern crate proc_macro;

use quote::quote;
use proc_macro::TokenStream;
use syn::{DeriveInput};
use syn::Data;

#[proc_macro_derive(IntoCbytes)]
pub fn into_cbytes_derivce(input: TokenStream) -> TokenStream {
    let ast: DeriveInput = syn::parse(input).unwrap();
    let name = ast.ident;
    let mut fields: Vec<proc_macro2::TokenStream> = Vec::new();

    match ast.data {
        Data::Struct(datastruct) => {
            for field in datastruct.fields {
                if let Some(name) = field.ident {
                    //handle Optional types
                    let gen = quote! {
                    v.extend(self.#name.into_cbytes());

                };
                    fields.push(gen);
                }
            }
        }
        _ => {}
    }


    let gen = quote! {
        impl IntoBytes for #name {
            fn into_cbytes(&self) -> Vec<u8> {
                let mut v: Vec<u8> = vec![];
                #(#fields)*
                v
            }
        }
    };
    gen.into()
}